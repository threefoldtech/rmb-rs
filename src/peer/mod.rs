use crate::identity::Signer;
use crate::twin::TwinDB;
use crate::types::{
    Address, Envelope, EnvelopeExt, Error as MessageError, Response, ValidationError,
};
use anyhow::{Context, Result};
use protobuf::Message as ProtoMessage;
use std::sync::Arc;
use std::time::Duration;
use storage::Storage;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

mod builtin;
mod con;
pub mod storage;
use con::{Connection, Writer};
use storage::{JsonIncomingRequest, JsonOutgoingRequest, JsonResponse};

use self::storage::JsonError;

#[derive(thiserror::Error, Debug)]
enum EnvelopeErrorKind {
    #[error("failed to validate envelope: {0}")]
    Validation(ValidationError),
    #[error("invalid signature: {0}")]
    InvalidSignature(anyhow::Error),
    #[error("failed to get twin information: {0}")]
    GetTwin(anyhow::Error),
    #[error("twin not found")]
    UnknownTwin,
    #[error("unknown built-in command '{0}'")]
    UnknownCommand(String),
    #[error("{0}")]
    Other(anyhow::Error),
}

impl EnvelopeErrorKind {
    fn code(&self) -> u32 {
        match self {
            Self::Validation(_) => 256,
            Self::InvalidSignature(_) => 257,
            Self::GetTwin(_) => 258,
            Self::UnknownTwin => 259,
            Self::UnknownCommand(_) => 260,
            Self::Other(_) => 300,
        }
    }
}

impl From<EnvelopeErrorKind> for JsonError {
    fn from(value: EnvelopeErrorKind) -> Self {
        Self {
            code: value.code(),
            message: value.to_string(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum PeerError {
    #[error("received invalid message type")]
    InvalidMessage,

    #[error("received invalid message format: {0}")]
    InvalidPayload(#[from] protobuf::Error),

    #[error("envelope error {0}")]
    Envelope(#[from] EnvelopeErrorKind),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl PeerError {
    fn code(&self) -> u32 {
        match self {
            Self::InvalidMessage => 1,
            Self::InvalidPayload(_) => 2,
            Self::Envelope(k) => k.code(),
            Self::Other(_) => 500,
        }
    }
}

impl From<PeerError> for JsonError {
    fn from(value: PeerError) -> Self {
        Self {
            code: value.code(),
            message: value.to_string(),
        }
    }
}

pub struct Peer<S, G, D>
where
    S: Storage,
    G: Signer + Clone,
    D: TwinDB + Clone,
{
    address: Address,
    storage: S,
    signer: G,
    // this is wrapped in an option
    // to support take()
    con: Option<Connection>,
    db: D,
}

impl<S, G, D> Peer<S, G, D>
where
    S: Storage,
    G: Signer + Clone + Send + Sync + 'static,
    D: TwinDB + Clone,
{
    pub async fn new(rely: Url, twin: u32, signer: G, storage: S, db: D) -> Self {
        let con = Connection::connect(rely, twin, signer.clone());
        let mut address = Address::new();
        address.twin = twin;
        Self {
            address,
            storage,
            signer,
            con: Some(con),
            db,
        }
    }

    fn parse(&self, msg: Message) -> Result<Option<Envelope>, PeerError> {
        let bytes = match msg {
            Message::Pong(_) => return Ok(None),
            Message::Binary(bytes) => bytes,
            _ => return Err(PeerError::InvalidMessage),
        };

        let envelope = Envelope::parse_from_bytes(&bytes)?;
        Ok(Some(envelope))
    }

    async fn handle_envelope(&self, envelope: Envelope) -> Result<(), PeerError> {
        envelope.valid().map_err(EnvelopeErrorKind::Validation)?;

        let twin = self
            .db
            .get_twin(envelope.source.twin)
            .await
            .map_err(EnvelopeErrorKind::GetTwin)?
            .ok_or(EnvelopeErrorKind::UnknownTwin)?;

        envelope
            .verify(&twin.account)
            .map_err(EnvelopeErrorKind::InvalidSignature)?;

        if envelope.has_request() {
            let request: JsonIncomingRequest = envelope
                .try_into()
                .context("failed to get request from envelope")?;
            return self
                .storage
                .run(request)
                .await
                .map_err(EnvelopeErrorKind::Other)
                .map_err(PeerError::Envelope);
        }

        log::trace!("received a response: {}", envelope.uid);
        // - get message from backlog
        // - fill back everything else from
        //   the backlog then push to reply queue
        let backlog = self
            .storage
            .get(&envelope.uid)
            .await
            .context("failed to get message backlog")?;

        let backlog = match backlog {
            Some(bl) => bl,
            None => {
                log::warn!("received reply of an expired message");
                return Ok(());
            }
        };

        let mut response: JsonResponse = envelope.try_into()?;
        // set the reference back to original value
        response.reference = backlog.reference;
        log::trace!("pushing response to reply queue: {}", backlog.reply_to);
        self.storage
            .reply(&backlog.reply_to, response)
            .await
            .context("failed to push received reply")?;
        Ok(())
    }

    // handler for incoming envelopes from the relay
    async fn handler(peer: Arc<Self>, mut reader: Connection) {
        while let Some(input) = reader.read().await {
            let envelope = match peer.parse(input) {
                Ok(Some(env)) => env,
                Ok(_) => continue,
                Err(err) => {
                    log::error!("error while loading received message: {:#}", err);
                    continue;
                }
            };

            // we track these here in case we need to send an error
            let uid = envelope.uid.clone();
            let source = envelope.source.clone();
            match peer.handle_envelope(envelope).await {
                Ok(_) => {}
                Err(PeerError::Envelope(kind)) => {
                    // while processing incoming envelope, error happened
                    // but this error happened after the envelope has been
                    // decoded, so we have enough information to actually send
                    // back an error response.
                    let mut e = MessageError::new();
                    e.code = kind.code();
                    e.message = kind.to_string();
                    let mut body = Response::new();
                    body.set_error(e);

                    let mut response = Envelope::new();
                    response.uid = uid;
                    response.destination = source;
                    response.set_response(body);
                    response.expiration = 300;

                    if let Err(err) = peer.send(&reader.writer(), response).await {
                        log::error!("failed to push error response back to caller: {:#}", err);
                    }
                }
                Err(err) => log::error!("error while handling received message: {:#}", err),
            };
        }
    }

    // handle outgoing requests
    async fn request(
        &self,
        writer: &Writer,
        request: JsonOutgoingRequest,
    ) -> Result<(), PeerError> {
        // generate an id?
        let uid = uuid::Uuid::new_v4().to_string();
        let (backlog, envelopes, ttl) = request.parts()?;
        self.storage
            .track(&uid, ttl, backlog)
            .await
            .context("failed to store message tracking information")?;

        for mut envelope in envelopes {
            envelope.uid = uid.clone();
            envelope.source = Some(self.address.clone()).into();

            self.send(writer, envelope).await?;
        }

        Ok(())
    }

    // handle outgoing requests (so sent by a client to the peer) but command is prefixed
    // with `rmb.` which makes it internal command. rmb can then process this differently
    // and send a reply back to caller.
    async fn request_builtin(
        &self,
        _writer: &Writer,
        request: JsonOutgoingRequest,
    ) -> Result<(), PeerError> {
        Err(EnvelopeErrorKind::UnknownCommand(request.command).into())
    }

    // handle outgoing responses
    async fn response(&self, writer: &Writer, response: JsonResponse) -> Result<()> {
        // that's a reply message that is initiated locally and need to be
        // sent to a remote peer
        self.send(
            writer,
            response
                .try_into()
                .context("failed to build envelope from response")?,
        )
        .await
    }

    async fn send(&self, writer: &Writer, mut envelope: Envelope) -> Result<()> {
        envelope.source = Some(self.address.clone()).into();
        envelope.stamp();
        envelope
            .ttl()
            .context("response has expired before sending!")?;
        envelope.sign(&self.signer);
        let bytes = envelope
            .write_to_bytes()
            .context("failed to serialize envelope")?;
        log::trace!(
            "pushing outgoing response: {} -> {:?}",
            envelope.uid,
            envelope.destination
        );
        writer.write(Message::Binary(bytes)).await?;

        Ok(())
    }

    pub async fn start(mut self) -> Result<()> {
        use tokio::time::sleep;
        let wait = Duration::from_secs(1);
        let con = self.con.take().expect("unreachable");
        let writer = con.writer();
        let pinger = con.writer();
        let peer = Arc::new(self);
        // start a processor for incoming message
        tokio::spawn(Self::handler(Arc::clone(&peer), con));

        // start a routine to send pings to server every 20 seconds
        tokio::spawn(async move {
            loop {
                if let Err(err) = pinger.write(Message::Ping(Vec::default())).await {
                    log::error!("ping error: {}", err);
                }
                sleep(Duration::from_secs(20)).await;
            }
        });

        loop {
            let msg = match peer.storage.messages().await {
                Ok(msg) => msg,
                Err(err) => {
                    log::error!("failed to process local messages: {:#}", err);
                    sleep(wait).await;
                    continue;
                }
            };

            let result = match msg {
                storage::JsonMessage::Response(response) => peer.response(&writer, response).await,
                storage::JsonMessage::Request(request) => {
                    let reply_to = request.reply_to.clone();
                    let reference = request.reference.clone();

                    let result = if request.command.starts_with("rmb.") {
                        peer.request_builtin(&writer, request).await
                    } else {
                        peer.request(&writer, request).await
                    };
                    // failure to process the request then we can simply
                    // push a response back directly to the client
                    if let Err(err) = result {
                        // we failed to process
                        // self.storage
                        peer.storage
                            .reply(
                                &reply_to,
                                JsonResponse {
                                    version: 1,
                                    reference: reference,
                                    data: String::default(),
                                    destination: String::default(),
                                    schema: None,
                                    timestamp: 0,
                                    error: Some(err.into()),
                                },
                            )
                            .await?
                    }

                    Ok(())
                }
            };

            if let Err(err) = result {
                log::error!("failed to process message: {}", err);
            }
        }
    }
}
