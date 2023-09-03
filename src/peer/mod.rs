use crate::identity::Signer;
use crate::twin::TwinDB;
use crate::types::Envelope;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use storage::Storage;
use tokio::sync::mpsc::Sender;
use url::Url;

mod postman;
mod protocol;
mod socket;

use postman::{Bag, Postman};

pub mod e2e;
pub mod plugins;
pub mod storage;

pub use e2e::Pair;
pub use plugins::Plugin;
pub use protocol::Peer;

use socket::Socket;
use storage::{
    JsonIncomingRequest, JsonIncomingResponse, JsonOutgoingRequest, JsonOutgoingResponse,
};

use self::protocol::{Protocol, ProtocolError};

type Plugins = HashMap<String, Box<dyn Plugin>>;

pub struct App<DB, S, R>
where
    DB: TwinDB,
    S: Signer,
    R: Storage,
{
    protocol: Protocol<DB, S>,
    storage: R,
    plugins: Plugins,
}

impl<DB, S, R> App<DB, S, R>
where
    DB: TwinDB + Clone,
    S: Signer,
    R: Storage,
{
    pub fn new(relays: Vec<Url>, peer: Peer<S>, twins: DB, storage: R) -> Self {
        // create low level socket, this takes care of the relay connection and reconnecting if connection
        // is lost. this include authentication with the relay and proving identity
        let sockets: Vec<Socket> = relays.iter().map(|relay: &Url| Socket::connect(relay.to_owned(), peer.id, peer.signer.clone())).collect();

        // create a higher level protocol (Envelope) on top of the socket
        let protocol = Protocol::new(sockets, peer, twins);

        Self {
            protocol,
            storage,
            plugins: HashMap::default(),
        }
    }

    pub fn plugin<P: Plugin>(&mut self, plugin: P) {
        self.plugins.insert(plugin.name().into(), Box::new(plugin));
    }

    pub async fn start(self) {
        // start the post man which is responsible for sending message with tracking
        let postman = Postman::new(self.protocol.writer(), self.storage.clone());
        let sender = postman.start();

        // start all modules
        let mut plugins = self.plugins;
        for plugin in plugins.values_mut() {
            plugin.start(sender.clone());
        }

        let plugins = Arc::new(plugins);

        // handle all local generate traffic and push it to relay
        let upstream = Upstream::new(sender.clone(), Arc::clone(&plugins), self.storage.clone());

        // handle all received messages from the relay
        let downstream = Downstream::new(sender.clone(), self.protocol, plugins, self.storage);

        // start a processor for incoming message
        tokio::spawn(downstream.start());

        // we start this in this current routine to block the peer from exiting
        // no need to spawn it in the back
        upstream.start().await;
    }
}

/// Upstream handle all local messages (requests and responses) from the storage and make
/// sure they are handed over to the postman
struct Upstream<R>
where
    R: Storage,
{
    up: Sender<Bag>,
    storage: R,
    plugins: Arc<Plugins>,
}

impl<R> Upstream<R>
where
    R: Storage,
{
    pub fn new(up: Sender<Bag>, plugins: Arc<Plugins>, storage: R) -> Self {
        Self {
            up,
            storage,
            plugins,
        }
    }

    // handle outgoing requests
    async fn request(&self, request: JsonOutgoingRequest) -> Result<()> {
        // check if request is intended for a module
        let request = match request.command.split_once('.') {
            // no prefix
            None => Some(request),
            Some((prefix, _)) => match self.plugins.get(prefix) {
                // there is a prefix that matches a module, feed request to module
                Some(plugin) => plugin.local(request).await,
                // no matching module
                None => Some(request),
            },
        };

        let request = match request {
            Some(request) => request,
            None => return Ok(()),
        };

        let (backlog, envelopes) = request.to_envelops()?;

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
            .send(Bag::one(envelope))
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
                storage::JsonMessage::Request(request) => self.request(request).await,
                storage::JsonMessage::Response(response) => self.response(response).await,
            };

            if let Err(err) = result {
                log::error!("failed to process message: {}", err);
            }
        }
    }
}

/// Downstream takes care of reading all received messages
/// then dispatch the received message based on type to either
/// the right module or a response queue to be consumed by the local
/// client that might be waiting for a response.
struct Downstream<DB, S, G>
where
    DB: TwinDB,
    S: Storage,
    G: Signer,
{
    up: Sender<Bag>,
    reader: Protocol<DB, G>,
    storage: S,
    plugins: Arc<Plugins>,
}

impl<DB, S, G> Downstream<DB, S, G>
where
    DB: TwinDB + Clone,
    S: Storage,
    G: Signer,
{
    pub fn new(
        up: Sender<Bag>,
        reader: Protocol<DB, G>,
        plugins: Arc<Plugins>,
        storage: S,
    ) -> Self {
        Self {
            up,
            reader,
            storage,
            plugins,
        }
    }

    async fn request(&self, envelope: &Envelope) -> Result<(), ProtocolError> {
        log::trace!("received a request: {}", envelope.uid);

        let request: JsonIncomingRequest = envelope
            .try_into()
            .context("failed to get request from envelope")?;
        self.storage
            .request(request)
            .await
            .map_err(ProtocolError::Other)
    }

    async fn response(&self, envelope: &Envelope) -> Result<(), ProtocolError> {
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

        if let Some(plugin) = self.plugins.get(&backlog.module) {
            plugin.remote(Some(backlog), envelope).await;
            return Ok(());
        }

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

    async fn handle(&self, envelope: &Envelope) -> Result<(), ProtocolError> {
        log::trace!(
            "received a message {} (req: {}, resp: {})",
            envelope.uid,
            envelope.has_request(),
            envelope.has_response()
        );

        let req = envelope.request();
        // is this for a module
        if let Some((prefix, _)) = req.command.split_once('.') {
            if let Some(plugin) = self.plugins.get(prefix) {
                plugin.remote(None, envelope).await;
                return Ok(());
            }
        }

        // otherwise this can either be a request envelope
        // for a local service
        if envelope.has_request() {
            return self.request(envelope).await;
        }

        self.response(envelope).await
    }

    // handler for incoming envelopes from the relay
    pub async fn start(mut self) {
        while let Some(envelope) = self.reader.read().await {
            if let Err(err) = self.handle(&envelope).await {
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

                    if let Err(err) = self.up.send(Bag::one(reply)).await {
                        log::error!("failed to send error response to caller: {}", err);
                    }
                }
            };
        }
    }
}
