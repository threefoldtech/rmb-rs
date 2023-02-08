use crate::identity::Signer;
use crate::twin::TwinDB;
use crate::types::{Address, Envelope, EnvelopeExt, Error as MessageError, ValidationError};
use anyhow::{Context, Result};
use protobuf::Message as ProtoMessage;
use std::time::Duration;
use storage::Storage;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

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
            Self::Validation(_) => 300,
            Self::InvalidSignature(_) => 301,
            Self::GetTwin(_) => 302,
            Self::UnknownTwin => 303,
            Self::UnknownCommand(_) => 304,
            Self::Other(_) => 305,
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
            // range 100
            Self::InvalidMessage => 100,
            // range 200
            Self::InvalidPayload(_) => 200,
            // range 300
            Self::Envelope(k) => k.code(),
            // range 500
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

/// entry point for peer, it initializes connection to the relay and handle both up stream
/// and down stream
/// - it uses the storage to get local generated requests or responses, and forward it to the relay
/// - it handle all received messages and dispatch it to local clients or services.
/// - sign all outgoing messages
/// - verify all incoming messages
/// - restore relay connection if lost
pub async fn start<S, G, DB>(relay: Url, twin: u32, signer: G, storage: S, db: DB) -> Result<()>
where
    S: Storage,
    G: Signer + Clone + Send + Sync + 'static,
    DB: TwinDB + Clone,
{
    let con = Connection::connect(relay, twin, signer.clone());
    let mut address = Address::new();
    address.twin = twin;

    // a high level sender that can stamp and sign the message before sending automatically
    let sender = Sender::new(con.writer(), address, signer);

    // handle all received messages from the relay
    let downstream = Downstream::new(db, storage.clone(), sender.clone());
    // handle all local generate traffic and push it to relay
    let upstream = Upstream::new(storage, sender);

    //let upstream = Upstream::
    // start a processor for incoming message
    tokio::spawn(downstream.start(con));

    // we start this in this current routine to block the peer from exiting
    // no need to spawn it in the back
    upstream.start().await;
    // shouldn't be reachable
    Ok(())
}

/// Upstream handle all local traffic and making sure to push
/// it to server (relay)
struct Upstream<S, G>
where
    S: Storage,
    G: Signer,
{
    storage: S,
    sender: Sender<G>,
}

impl<S, G> Upstream<S, G>
where
    S: Storage,
    G: Signer,
{
    pub fn new(storage: S, sender: Sender<G>) -> Self {
        Self { storage, sender }
    }

    // handle outgoing requests
    async fn request(&self, request: JsonOutgoingRequest) -> Result<(), PeerError> {
        // generate an id?
        let uid = uuid::Uuid::new_v4().to_string();
        let (backlog, envelopes, ttl) = request.parts()?;
        self.storage
            .track(&uid, ttl, backlog)
            .await
            .context("failed to store message tracking information")?;

        for mut envelope in envelopes {
            envelope.uid = uid.clone();
            self.sender.send(envelope).await?;
        }

        Ok(())
    }

    // handle outgoing requests (so sent by a client to the peer) but command is prefixed
    // with `rmb.` which makes it internal command. rmb can then process this differently
    // and send a reply back to caller.
    async fn request_builtin(&self, request: JsonOutgoingRequest) -> Result<(), PeerError> {
        Err(EnvelopeErrorKind::UnknownCommand(request.command).into())
    }

    // handle outgoing responses
    async fn response(&self, response: JsonResponse) -> Result<()> {
        // that's a reply message that is initiated locally and need to be
        // sent to a remote peer
        self.sender
            .send(
                response
                    .try_into()
                    .context("failed to build envelope from response")?,
            )
            .await
    }

    pub async fn start(self) {
        let wait = Duration::from_secs(1);
        loop {
            let msg = match self.storage.messages().await {
                Ok(msg) => msg,
                Err(err) => {
                    log::error!("failed to process local messages: {:#}", err);
                    tokio::time::sleep(wait).await;
                    continue;
                }
            };

            let result = match msg {
                storage::JsonMessage::Response(response) => self.response(response).await,
                storage::JsonMessage::Request(request) => {
                    let reply_to = request.reply_to.clone();
                    let reference = request.reference.clone();

                    let result = if request.command.starts_with("rmb.") {
                        self.request_builtin(request).await
                    } else {
                        self.request(request).await
                    };
                    // failure to process the request then we can simply
                    // push a response back directly to the client
                    match result {
                        Ok(_) => Ok(()),
                        Err(err) => {
                            // we have enough information to send an erro
                            // response back to the local caller
                            self.storage
                                .reply(
                                    &reply_to,
                                    JsonResponse {
                                        version: 1,
                                        reference,
                                        data: String::default(),
                                        destination: String::default(),
                                        schema: None,
                                        timestamp: 0,
                                        error: Some(err.into()),
                                    },
                                )
                                .await
                        }
                    }
                }
            };

            if let Err(err) = result {
                log::error!("failed to process message: {}", err);
            }
        }
    }
}

/// downstream is handler for the connection down stream
/// so basically anything that is received from the server (relay)
/// and making sure to validate and dispatch it as needed.
struct Downstream<DB, S, G>
where
    DB: TwinDB,
    S: Storage,
    G: Signer,
{
    db: DB,
    storage: S,
    sender: Sender<G>,
}

impl<DB, S, G> Downstream<DB, S, G>
where
    DB: TwinDB,
    S: Storage,
    G: Signer,
{
    pub fn new(db: DB, storage: S, sender: Sender<G>) -> Self {
        Self {
            db,
            storage,
            sender,
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
    pub async fn start(self, mut reader: Connection) {
        while let Some(input) = reader.read().await {
            let envelope = match self.parse(input) {
                Ok(Some(env)) => env,
                Ok(_) => {
                    log::trace!("received a pong message");
                    continue;
                }
                Err(err) => {
                    log::error!("error while loading received message: {:#}", err);
                    continue;
                }
            };

            // we track these here in case we need to send an error
            let uid = envelope.uid.clone();
            let source = envelope.source.clone();
            match self.handle_envelope(envelope).await {
                Ok(_) => {}
                Err(PeerError::Envelope(kind)) => {
                    // while processing incoming envelope, error happened
                    // but this error happened after the envelope has been
                    // decoded, so we have enough information to actually send
                    // back an error response.
                    let mut e = MessageError::new();
                    e.code = kind.code();
                    e.message = kind.to_string();

                    let mut response = Envelope::new();
                    response.set_error(e);
                    response.uid = uid;
                    response.destination = source;
                    response.expiration = 300;

                    if let Err(err) = self.sender.send(response).await {
                        log::error!("failed to push error response back to caller: {:#}", err);
                    }
                }
                Err(err) => log::error!("error while handling received message: {:#}", err),
            };
        }
    }
}

#[derive(Clone)]
struct Sender<S>
where
    S: Signer,
{
    writer: Writer,
    source: Address,
    signer: S,
}

impl<S> Sender<S>
where
    S: Signer + Clone,
{
    pub fn new(writer: Writer, source: Address, signer: S) -> Self {
        Self {
            writer,
            source,
            signer,
        }
    }

    /// send an envelope, make sure to stamp, and sign the envelope
    pub async fn send(&self, mut envelope: Envelope) -> Result<()> {
        envelope.source = Some(self.source.clone()).into();
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
        self.writer.write(Message::Binary(bytes)).await?;

        Ok(())
    }
}
