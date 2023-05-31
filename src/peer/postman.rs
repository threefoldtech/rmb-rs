use super::protocol::{ProtocolError, Writer};
use super::storage::{JsonIncomingResponse, Storage};
use crate::twin::TwinDB;
use crate::types::Envelope;
use crate::{identity::Signer, types::Backlog};
use anyhow::{Context, Result};
use tokio::sync::mpsc::{channel, Receiver, Sender};

const DEFAULT_TTL: u64 = 300;

#[async_trait::async_trait]
pub trait Generator {
    async fn next(&mut self) -> Option<Envelope>;
    //todo: add size hint?
}

#[async_trait::async_trait]
impl<T: Iterator<Item = Envelope> + Send + Sync> Generator for T {
    async fn next(&mut self) -> Option<Envelope> {
        self.next()
    }
}

/// A Bag is a set of envelops that need to be sent out with an optional backlog (tracker)
/// the tracker tell us who the sender is and where we need to respond back in case of an
/// error or a response
pub struct Bag {
    backlog: Option<Backlog>,
    envelops: Box<dyn Generator + Send + Sync + 'static>,
}

impl Bag {
    pub fn new<I>(envelops: I) -> Self
    where
        I: Generator + Send + Sync + 'static,
    {
        Bag {
            backlog: None,
            envelops: Box::new(envelops),
        }
    }

    pub fn one(env: Envelope) -> Self {
        Self::new(std::iter::once(env))
    }

    pub fn backlog(mut self, backlog: Backlog) -> Self {
        self.backlog = Some(backlog);
        self
    }
}

/// Postman works on top of the envelope protocol to send all envelops that need to be sent remotely
/// and actually send them. Postman is an app level layer on top of the protocol to also track local
/// requests. By keeping a tracker on local storage (if tracker) hance it can send back app level errors
/// back to the local caller (like undeliverable errors) automatically
///
/// this allow other components to send messages and forget about it, if the message fails
/// the post man will take care of informing the concerned entities
pub struct Postman<DB, S, R>
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
    pub fn new(writer: Writer<DB, S>, storage: R) -> Self {
        Self { writer, storage }
    }

    async fn push_one(&self, mut bag: Bag) -> Result<()> {
        if let Some(ref mut backlog) = bag.backlog {
            // a backlog help the system figure out where to route
            // a message if we received a response with that id.
            if backlog.ttl == 0 {
                backlog.ttl = DEFAULT_TTL;
            }
            self.storage
                .track(backlog)
                .await
                .context("failed to store message tracking information")?;
        }

        // TODO: validate that ALL envelope has the same id as the backlog
        // if backlog is set.
        // TODO: if a bag has MANY message (say a big file upload) other sender
        // might starve. We probably need to multiplex this somehow
        // a possible solution is to spawn a separate route for each bag if the
        // size of the bag is bigger than a specific size
        while let Some(mut envelope) = bag.envelops.next().await {
            log::trace!(
                "sending message {} dest({:?})",
                envelope.uid,
                envelope.destination
            );

            if envelope.expiration == 0 {
                envelope.expiration = 300;
            }

            if let Err(err) = self.writer.write(envelope).await {
                log::error!("failed to send message: {:#}", err);
                if let Err(err) = self.undeliverable(err, bag.backlog.as_ref()).await {
                    log::error!("failed to report send error to local caller: {:#}", err);
                }
            }
        }

        Ok(())
    }

    ///sends an error back to the caller if any only if the message is tracked via a backlog
    async fn undeliverable(&self, err: ProtocolError, backlog: Option<&Backlog>) -> Result<()> {
        let backlog = match backlog {
            Some(backlog) => backlog,
            // nothing we can do
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
                log::error!("failed to push message: {:#}", err);
            }
        }
    }

    /// starts the postman and return a sender channel that can be then
    /// used to ask the postman to deliver a bag of messages.
    pub fn start(self) -> Sender<Bag> {
        let (sender, receiver) = channel(1);

        tokio::spawn(self.push(receiver));

        sender
    }
}
