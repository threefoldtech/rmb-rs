use super::e2e::{Error as CryptoError, Pair};
use super::socket::{Socket, SocketWriter};
use crate::types::Address;
use crate::{
    identity::Signer,
    twin::TwinDB,
    types::{Envelope, EnvelopeExt, ValidationError},
};
use anyhow::Context;
use async_stream::stream;
use protobuf::Message as ProtoMessage;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tokio_tungstenite::tungstenite::Message;

#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    #[error("invalid message payload type")]
    InvalidMessage,

    #[error("invalid message payload data: {0}")]
    InvalidPayload(#[from] protobuf::Error),

    #[error("envelope validation error: {0}")]
    Validation(#[from] ValidationError),

    #[error("unknown twin id '{0}'")]
    UnknownTwin(u32),

    #[error("invalid envelope signature: {0}")]
    InvalidSignature(anyhow::Error),

    #[error("sender twin has no public key")]
    NoPublicKey,

    #[error("e2e error: {0}")]
    Crypto(#[from] CryptoError),

    #[error("other: {0}")]
    Other(#[from] anyhow::Error),
}

impl ProtocolError {
    pub fn code(&self) -> u32 {
        match self {
            ProtocolError::InvalidMessage => 100,
            ProtocolError::InvalidPayload(_) => 101,
            ProtocolError::Validation(_) => 102,
            ProtocolError::UnknownTwin(_) => 103,
            ProtocolError::InvalidSignature(_) => 104,
            ProtocolError::NoPublicKey => 105,
            ProtocolError::Crypto(_) => 106,
            ProtocolError::Other(_) => 500,
        }
    }
}

/// Peer holds the identity information about this peer
/// this include signing and crypt keys, and also the
/// id as registered on the chain
#[derive(Clone)]
pub struct Peer<S>
where
    S: Signer,
{
    pub id: u32,
    pub signer: S,
    pub crypto: Pair,
}

impl<S> Peer<S>
where
    S: Signer,
{
    pub fn new(id: u32, signer: S, crypto: Pair) -> Self {
        Self { id, signer, crypto }
    }
}

type Streams =
    StreamMap<usize, Pin<Box<dyn futures_util::Stream<Item = Message> + std::marker::Send>>>;

/// Protocol works on top of the low level
/// socket connection and implement Envelope protocol
/// it takes care of serialization/deserialization of messages
/// signing and verification of messages.
///
/// it also takes care of filling up source address, and destination federation
/// information, and timestamps
///
/// It also will handle protocol level errors by sending back protocol related
/// errors back to the sender if happens.
pub struct Protocol<DB, S>
where
    DB: TwinDB,
    S: Signer,
{
    inner: Arc<Mutex<Streams>>,
    twins: DB,
    peer: Peer<S>,

    writer: Writer<DB, S>,
}

impl<DB, S> Protocol<DB, S>
where
    DB: TwinDB + Clone,
    S: Signer,
{
    pub fn new(sockets: Vec<Socket>, peer: Peer<S>, twins: DB) -> Self {
        let writers = sockets.iter().map(|socket| socket.writer()).collect();
        let writer = Writer {
            inner: writers,
            identity: peer.clone(),
            twins: twins.clone(),
        };
        let mut streams = StreamMap::new();

        sockets.into_iter().enumerate().for_each(|(i, mut socket)| {
            let s = Box::pin(stream! {
                while let Some(msg) = socket.read().await {
                    yield msg
                }
            }) as Pin<Box<dyn Stream<Item = Message> + Send>>;
            streams.insert(i, s);
        });

        Self {
            inner: Arc::new(Mutex::new(streams)),
            twins,
            peer,
            writer,
        }
    }

    pub fn writer(&self) -> Writer<DB, S> {
        self.writer.clone()
    }
}

impl<DB, S> Protocol<DB, S>
where
    DB: TwinDB,
    S: Signer,
{
    fn parse(&self, msg: Message) -> Result<Envelope, ProtocolError> {
        let bytes = match msg {
            Message::Binary(bytes) => bytes,
            _ => return Err(ProtocolError::InvalidMessage),
        };

        let envelope = Envelope::parse_from_bytes(&bytes)?;
        Ok(envelope)
    }

    async fn verify(&self, envelope: &mut Envelope) -> Result<(), ProtocolError> {
        envelope.valid()?;
        if envelope.source.twin == 0 {
            // if source twin id is 0 then this is unsigned message from the relay ( an error report)
            return Ok(());
        }
        let twin = self
            .twins
            .get_twin(envelope.source.twin.into())
            .await
            .context("failed to get twin information")?
            .ok_or(ProtocolError::UnknownTwin(envelope.source.twin))?;

        envelope
            .verify(&twin.account)
            .map_err(ProtocolError::InvalidSignature)?;

        if envelope.has_cipher() {
            match twin.pk {
                Some(ref pk) => {
                    log::trace!("decrypt message from: {}", twin.id);
                    let plain = self.peer.crypto.decrypt(pk, envelope.cipher())?;
                    envelope.set_plain(plain);
                }
                None => {
                    return Err(ProtocolError::NoPublicKey);
                }
            }
        }

        Ok(())
    }

    pub async fn read(&mut self) -> Option<Envelope> {
        let mut streams = self.inner.lock().await;

        while let Some(msg) = streams.next().await {
            let mut envelope = match self.parse(msg.1) {
                Ok(env) => env.clone(),
                Err(err) => {
                    // if parse failed there is nothing we can do except
                    // logging the error.
                    log::error!("received invalid message: {}", err);
                    continue;
                }
            };

            // okay, no envelope has been parse correctly we "should"
            // have enough information to send an error if validation of this
            // envelope failed!
            match self.verify(&mut envelope).await {
                Ok(_) => return Some(envelope),
                Err(err) => {
                    log::error!("failed to process incoming message: {}", err);

                    if envelope.has_request() {
                        let mut reply = Envelope {
                            uid: envelope.uid,
                            destination: envelope.source,
                            expiration: envelope.expiration,
                            ..Default::default()
                        };

                        let e = reply.mut_error();
                        e.code = err.code();
                        e.message = err.to_string();
                        reply.stamp();
                        if let Err(err) = self.writer.write(reply).await {
                            log::error!("failed to send error response to sender: {}", err);
                        }
                    }
                }
            }
        }
        None
    }
}

#[derive(Clone)]
pub struct Writer<DB, S>
where
    DB: TwinDB,
    S: Signer,
{
    inner: Vec<SocketWriter>,
    twins: DB,
    identity: Peer<S>,
}

impl<DB, S> Writer<DB, S>
where
    DB: TwinDB,
    S: Signer,
{
    pub async fn write(&mut self, mut envelope: Envelope) -> Result<(), ProtocolError> {
        let twin = self
            .twins
            .get_twin(envelope.destination.twin.into())
            .await?
            .ok_or_else(|| ProtocolError::UnknownTwin(envelope.destination.twin))?;

        // if the other peer supports e2e we
        // also encrypt the message
        if let Some(ref pk) = twin.pk {
            log::trace!("encrypt message for: {}", twin.id);
            match self
                .identity
                .crypto
                .encrypt(pk, envelope.plain())
                .map_err(ProtocolError::Crypto)
            {
                Ok(cipher) => {
                    // if we managed to cipher the message
                    // we set it as payload
                    envelope.set_cipher(cipher);
                }
                Err(err) => {
                    // otherwise, we clear up the payload
                    // and set the error instead
                    envelope.payload = None;
                    let e = envelope.mut_error();
                    e.code = err.code();
                    e.message = err.to_string();
                }
            };
        }

        let address = Address {
            twin: self.identity.id,
            ..Default::default()
        };

        envelope.source = Some(address).into();
        envelope.stamp();
        envelope
            .ttl()
            .context("response has expired before sending!")?;
        envelope.sign(&self.identity.signer);
        let bytes = envelope
            .write_to_bytes()
            .context("failed to serialize envelope")?;
        log::trace!(
            "pushing outgoing response: {} -> {:?}",
            envelope.uid,
            envelope.destination
        );
        // we will keep trying all registered sockets till we succeed or message expired
        for (index, socket) in self.inner.iter().enumerate().cycle() {
            if socket
                .write(
                    Message::Binary(bytes.clone()),
                    Some(Duration::from_millis(500)),
                )
                .await
                .is_ok()
            {
                if index != 0 {
                    self.inner.swap(index, 0);
                }
                break;
            }
            log::warn!("using socket {} failed", index);
            // calm down after trying all sockets before repeating
            if index == self.inner.len() - 1 {
                sleep(Duration::from_secs(1)).await;
                envelope
                    .ttl()
                    .context("response has expired before sending!")?;
            }
        }

        Ok(())
    }
}
