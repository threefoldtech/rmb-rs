use crate::identity::Signer;
use crate::twin::TwinDB;
use crate::types::Envelope;
use anyhow::{Context, Result};
use std::time::Duration;
use storage::Storage;
use url::Url;

mod protocol;
mod socket;

pub mod e2e;
pub mod storage;
pub use e2e::Pair;

pub use protocol::Peer;

use socket::Socket;
use storage::{
    JsonIncomingRequest, JsonIncomingResponse, JsonOutgoingRequest, JsonOutgoingResponse,
};

use self::protocol::{Connection, ProtocolError, Writer};

/// entry point for peer, it initializes connection to the relay and handle both up stream
/// and down stream
/// - it uses the storage to get local generated requests or responses, and forward it to the relay
/// - it handle all received messages and dispatch it to local clients or services.
/// - sign all outgoing messages
/// - verify all incoming messages
/// - restore relay connection if lost
pub async fn start<S, G, DB>(relay: Url, peer: Peer<G>, storage: S, db: DB) -> Result<()>
where
    S: Storage,
    G: Signer + Clone + Send + Sync + 'static,
    DB: TwinDB + Clone,
{
    let socket = Socket::connect(relay, peer.id, peer.signer.clone());
    let connection = Connection::new(socket, peer, db);

    // handle all local generate traffic and push it to relay
    let upstream = Upstream::new(connection.writer(), storage.clone());
    // handle all received messages from the relay
    let downstream = Downstream::new(connection, storage);

    // start a processor for incoming message
    tokio::spawn(downstream.start());

    // we start this in this current routine to block the peer from exiting
    // no need to spawn it in the back
    upstream.start().await;
    // shouldn't be reachable
    Ok(())
}

/// Upstream handle all local traffic and making sure to push
/// it to server (relay)
struct Upstream<DB, S, G>
where
    DB: TwinDB,
    S: Storage,
    G: Signer,
{
    writer: Writer<DB, G>,
    storage: S,
}

impl<DB, S, G> Upstream<DB, S, G>
where
    DB: TwinDB,
    S: Storage,
    G: Signer,
{
    pub fn new(writer: Writer<DB, G>, storage: S) -> Self {
        Self { writer, storage }
    }

    // handle outgoing requests
    async fn request(&self, request: JsonOutgoingRequest) -> Result<(), ProtocolError> {
        // generate an id?
        let uid = uuid::Uuid::new_v4().to_string();
        let (backlog, envelopes, ttl) = request.parts()?;
        self.storage
            .track(&uid, ttl, &backlog)
            .await
            .context("failed to store message tracking information")?;

        for mut envelope in envelopes {
            envelope.uid = uid.clone();

            if let Err(err) = self.writer.write(envelope).await {
                //TODO: push error back as local response to this message
                if let Err(err) = self
                    .reply_err(err, &backlog.reply_to, &backlog.reference)
                    .await
                {
                    log::error!("failed to report send error to local caller: {}", err);
                }
            }
        }

        Ok(())
    }

    // handle outgoing requests (so sent by a client to the peer) but command is prefixed
    // with `rmb.` which makes it internal command. rmb can then process this differently
    // and send a reply back to caller.
    async fn request_builtin(&self, _request: JsonOutgoingRequest) -> Result<(), ProtocolError> {
        Err(ProtocolError::Other(anyhow::anyhow!("unknown command")))
    }

    // handle outgoing responses
    async fn response(&self, response: JsonOutgoingResponse) -> Result<(), ProtocolError> {
        // that's a reply message that is initiated locally and need to be
        // sent to a remote peer
        let envelope: Envelope = response
            .try_into()
            .context("failed to build envelope from response")?;

        self.writer.write(envelope).await
    }

    async fn reply_err(
        &self,
        err: ProtocolError,
        reply_to: &str,
        reference: &Option<String>,
    ) -> Result<()> {
        // error here can be a "multi-error"
        // in that case we need to send a full message
        // for each error in that list
        self.storage
            .response(
                reply_to,
                JsonIncomingResponse {
                    version: 1,
                    reference: reference.clone(),
                    data: String::default(),
                    source: String::default(),
                    schema: None,
                    timestamp: 0,
                    error: Some(err.into()),
                },
            )
            .await?;

        Ok(())
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
                    if request.command.starts_with("rmb.") {
                        self.request_builtin(request).await
                    } else {
                        self.request(request).await
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
    reader: Connection<DB, G>,
    storage: S,
}

impl<DB, S, G> Downstream<DB, S, G>
where
    DB: TwinDB + Clone,
    S: Storage,
    G: Signer,
{
    pub fn new(reader: Connection<DB, G>, storage: S) -> Self {
        Self { reader, storage }
    }

    async fn handle_envelope(&self, envelope: &Envelope) -> Result<(), ProtocolError> {
        if envelope.has_request() {
            let request: JsonIncomingRequest = envelope
                .try_into()
                .context("failed to get request from envelope")?;
            return self
                .storage
                .request(request)
                .await
                .map_err(ProtocolError::Other);
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

        let mut response: JsonIncomingResponse = envelope.try_into()?;
        // set the reference back to original value
        response.reference = backlog.reference;
        log::trace!("pushing response to reply queue: {}", backlog.reply_to);
        self.storage
            .response(&backlog.reply_to, response)
            .await
            .context("failed to push received reply")?;
        Ok(())
    }

    // handler for incoming envelopes from the relay
    pub async fn start(mut self) {
        let writer = self.reader.writer();

        while let Some(envelope) = self.reader.read().await {
            if let Err(err) = self.handle_envelope(&envelope).await {
                // process error here if exists.
                if envelope.has_request() {
                    let mut reply = Envelope {
                        uid: envelope.uid,
                        destination: envelope.source,
                        expiration: 300,
                        ..Default::default()
                    };

                    let e = reply.mut_error();
                    e.message = err.to_string();

                    if let Err(err) = writer.write(reply).await {
                        log::error!("failed to send error response to caller: {}", err);
                    }
                }
            };
        }
    }
}
