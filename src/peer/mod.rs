use crate::twin::TwinDB;
use crate::types::Envelope;
use crate::{identity::Signer, types::Backlog};
use anyhow::{Context, Result};
use std::time::Duration;
use storage::Storage;
use tokio::sync::mpsc::{channel, Receiver, Sender};
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

pub struct App<DB, S, R>
where
    DB: TwinDB,
    S: Signer,
    R: Storage,
{
    connection: Connection<DB, S>,
    storage: R,
}

impl<DB, S, R> App<DB, S, R>
where
    DB: TwinDB + Clone,
    S: Signer,
    R: Storage,
{
    pub fn new(relay: Url, peer: Peer<S>, twins: DB, storage: R) -> Self {
        let socket = Socket::connect(relay, peer.id, peer.signer.clone());
        let connection = Connection::new(socket, peer, twins);

        Self {
            connection,
            storage,
        }
    }

    pub async fn start(self) {
        let (sender, receiver) = channel(1);

        let postman = Postman::new(self.connection.writer(), self.storage.clone());
        tokio::spawn(postman.push(receiver));

        // handle all local generate traffic and push it to relay
        let upstream = Upstream::new(sender.clone(), self.storage.clone());
        // handle all received messages from the relay
        let downstream = Downstream::new(self.connection, self.storage);

        // start a processor for incoming message
        tokio::spawn(downstream.start());

        // we start this in this current routine to block the peer from exiting
        // no need to spawn it in the back
        upstream.start().await;
    }
}

/// A Bag is a set of envelops that need to be sent out with an optional backlog (tracker)
/// the tracker tell us who the sender is and where we need to respond back in case of an
/// error or a response
struct Bag {
    backlog: Option<Backlog>,
    envelops: Box<dyn Iterator<Item = Envelope> + Send + Sync + 'static>,
}

impl Bag {
    pub fn new<I>(envelops: I) -> Self
    where
        I: Iterator<Item = Envelope> + Send + Sync + 'static,
    {
        Bag {
            backlog: None,
            envelops: Box::new(envelops),
        }
    }

    pub fn backlog(mut self, backlog: Backlog) -> Self {
        self.backlog = Some(backlog);
        self
    }
}

/// Postman is responsible of receiving all envelops that need to be sent remotely
/// and actually send them. If a message is failed to deliver a response is send
/// back locally for the caller (over storage) to explain why
struct Postman<DB, S, R>
where
    DB: TwinDB,
    S: Signer,
    R: Storage,
{
    writer: Writer<DB, S>,
    storage: R,
}

impl<DB, S, R> Postman<DB, S, R>
where
    DB: TwinDB,
    S: Signer,
    R: Storage,
{
    fn new(writer: Writer<DB, S>, storage: R) -> Self {
        Self { writer, storage }
    }

    async fn push_one(&self, bag: Bag) -> Result<()> {
        if let Some(ref backlog) = bag.backlog {
            self.storage
                .track(&backlog)
                .await
                .context("failed to store message tracking information")?;
        }

        // TODO: validate that ALL envelope has the same id as the backlog
        // if backlog is set.
        for envelope in bag.envelops {
            if let Err(err) = self.writer.write(envelope).await {
                if let Err(err) = self.undeliverable(err, bag.backlog.as_ref()).await {
                    log::error!("failed to report send error to local caller: {}", err);
                }
            }
        }

        Ok(())
    }

    ///sends an error back to the caller if any only if the message is tracked via a backlog
    async fn undeliverable(&self, err: ProtocolError, backlog: Option<&Backlog>) -> Result<()> {
        let backlog = match backlog {
            Some(backlog) => backlog,
            None => return Ok(()),
        };

        self.storage
            .response(
                &backlog.reply_to,
                JsonIncomingResponse {
                    version: 1,
                    reference: backlog.reference.clone(),
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

    async fn push(self, mut rx: Receiver<Bag>) {
        while let Some(sent) = rx.recv().await {
            if let Err(err) = self.push_one(sent).await {
                log::error!("failed to push message: {}", err);
            }
        }
    }
}

/// Upstream handle all local traffic and making sure to push
/// it to server (relay)
struct Upstream<R>
where
    R: Storage,
{
    up: Sender<Bag>,
    storage: R,
}

impl<R> Upstream<R>
where
    R: Storage,
{
    pub fn new(up: Sender<Bag>, storage: R) -> Self {
        Self { up, storage }
    }

    // handle outgoing requests
    async fn request(&self, request: JsonOutgoingRequest) -> Result<()> {
        // generate an id?
        let (backlog, envelopes) = request.parts()?;

        self.up
            .send(Bag::new(envelopes).backlog(backlog))
            .await
            .map_err(|_| anyhow::anyhow!("failed to deliver request message(s) to the postman"))
    }

    // handle outgoing responses
    async fn response(&self, response: JsonOutgoingResponse) -> Result<()> {
        // that's a reply message that is initiated locally and need to be
        // sent to a remote peer
        let envelope: Envelope = response
            .try_into()
            .context("failed to build envelope from response")?;

        self.up
            .send(Bag::new(std::iter::once(envelope)))
            .await
            .map_err(|_| anyhow::anyhow!("failed to deliver response message to the postman"))
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
                storage::JsonMessage::Request(request) => self.request(request).await,
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
                        tags: envelope.tags,
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
