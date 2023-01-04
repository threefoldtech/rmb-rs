use crate::identity::Signer;
use crate::types::{Envelope, EnvelopeExt, JsonRequest, JsonResponse};
use anyhow::{Context, Result};
use protobuf::Message as ProtoMessage;
use std::time::Duration;
use storage::Storage;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

mod con;
pub mod storage;
use con::Connection;

pub struct Peer<S: Storage, G: Signer> {
    id: u32,
    storage: S,
    signer: G,
    con: Connection,
}

impl<S, G> Peer<S, G>
where
    S: Storage,
    G: Signer + Clone + Send + Sync + 'static,
{
    pub async fn new(rely: Url, id: u32, storage: S, signer: G) -> Self {
        let con = Connection::connect(rely, id, signer.clone());
        Self {
            id,
            storage,
            signer,
            con,
        }
    }

    async fn process_msg(storage: &S, con: &mut Connection, msg: Message) -> Result<()> {
        let bytes = match msg {
            Message::Ping(m) => {
                con.write(Message::Pong(m)).await?;
                return Ok(());
            }
            Message::Pong(_) => return Ok(()),
            Message::Binary(bytes) => bytes,
            _ => {
                anyhow::bail!("received invalid message (not binary)")
            }
        };

        let envelope = Envelope::parse_from_bytes(&bytes).context("received invalid envelope")?;

        envelope.valid().context("error validating envelope")?;
        // todo: envelope signature validation goes here
        // dispatch message back to either

        if envelope.has_request() {
            let request: JsonRequest = envelope
                .try_into()
                .context("failed to get request from envelope")?;
            return storage
                .run(request)
                .await
                .context("failed to schedule request to run");
        }
        // - get message from backlog
        // - fill back everything else from
        //   the backlog then push to reply queue
        let backlog = storage
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

        storage.reply(&backlog.reply_to, response).await
    }

    async fn process(storage: S, mut reader: Connection) {
        while let Some(input) = reader.read().await {
            if let Err(err) = Self::process_msg(&storage, &mut reader, input).await {
                log::error!("error while handling received message: {:#}", err);
                continue;
            }
        }
    }

    pub async fn start(self) -> Result<()> {
        use tokio::time::sleep;
        let wait = Duration::from_secs(1);
        let writer = self.con.writer();
        tokio::spawn(Self::process(self.storage.clone(), self.con));

        loop {
            let mut envelope = match self.storage.messages().await {
                Ok(msg) => msg,
                Err(err) => {
                    log::error!("failed to process local messages: {:#}", err);
                    sleep(wait).await;
                    continue;
                }
            };

            envelope.stamp();
            if envelope.ttl().is_none() {
                // todo: if this is a request we can immediately return a
                // an error
                log::warn!(
                    "message with id({}, {}) has expired",
                    envelope.uid,
                    envelope.reference
                );
                continue;
            }

            if envelope.has_request() {
                envelope.source = self.id;
                self.storage.track(&envelope).await?;
            }

            envelope.sign(&self.signer);
            // envelope.sign(self.id)
            let bytes = match envelope.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => {
                    log::error!("failed to serialize envelope: {}", err);
                    continue;
                }
            };

            if let Err(err) = writer.write(Message::Binary(bytes)).await {
                log::error!("failed to queue message for sending: {}", err);
            }
        }
    }
}
