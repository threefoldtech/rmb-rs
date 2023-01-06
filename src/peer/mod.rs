use crate::identity::Signer;
use crate::twin::TwinDB;
use crate::types::{Envelope, EnvelopeExt};
use anyhow::{Context, Result};
use protobuf::Message as ProtoMessage;
use std::time::Duration;
use storage::Storage;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

mod con;
pub mod storage;
use con::{Connection, Writer};
use storage::{JsonRequest, JsonResponse};

pub struct Peer<S, G, D>
where
    S: Storage,
    G: Signer + Clone,
    D: TwinDB + Clone,
{
    id: u32,
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
    pub async fn new(rely: Url, id: u32, signer: G, storage: S, db: D) -> Self {
        let con = Connection::connect(rely, id, signer.clone());
        Self {
            id,
            storage,
            signer,
            con: Some(con),
            db,
        }
    }

    async fn process_msg(db: &D, storage: &S, msg: Message) -> Result<()> {
        let bytes = match msg {
            Message::Pong(_) => return Ok(()),
            Message::Binary(bytes) => bytes,
            _ => {
                anyhow::bail!("received invalid message (not binary)")
            }
        };

        let envelope = Envelope::parse_from_bytes(&bytes).context("received invalid envelope")?;

        envelope.valid().context("error validating envelope")?;
        let twin = db
            .get_twin(envelope.source)
            .await
            .context("failed to get twin")?
            .with_context(|| format!("unknown twin: {}", envelope.source))?;

        envelope
            .verify(&twin.account)
            .context("invalid signature")?;

        if envelope.has_request() {
            let request: JsonRequest = envelope
                .try_into()
                .context("failed to get request from envelope")?;
            return storage
                .run(request)
                .await
                .context("failed to schedule request to run");
        }

        log::trace!("received a response: {}", envelope.uid);
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
        log::trace!("pushing response to reply queue: {}", backlog.reply_to);
        storage.reply(&backlog.reply_to, response).await
    }

    async fn process(db: D, storage: S, mut reader: Connection) {
        while let Some(input) = reader.read().await {
            if let Err(err) = Self::process_msg(&db, &storage, input).await {
                log::error!("error while handling received message: {:#}", err);
                continue;
            }
        }
    }

    // handle outgoing requests
    async fn request(&self, writer: &Writer, request: JsonRequest) -> Result<()> {
        // generate an id?
        let uid = uuid::Uuid::new_v4().to_string();
        let (backlog, envelopes, ttl) = request.parts()?;
        self.storage
            .track(&uid, ttl, backlog)
            .await
            .context("failed to store message tracking information")?;

        for mut envelope in envelopes {
            envelope.uid = uid.clone();
            envelope.source = self.id;
            envelope.stamp();
            envelope.ttl().context("message has expired")?;
            envelope.sign(&self.signer);
            let bytes = envelope
                .write_to_bytes()
                .context("failed to serialize envelope")?;

            log::trace!(
                "pushing outgoing request: {} -> {}",
                envelope.uid,
                envelope.destination
            );

            writer.write(Message::Binary(bytes)).await?;
        }

        Ok(())
    }

    // handle outgoing responses
    async fn response(&self, writer: &Writer, response: JsonResponse) -> Result<()> {
        // that's a reply message that is initiated locally and need to be
        // sent to a remote peer
        let mut envelope: Envelope = response
            .try_into()
            .context("failed to build envelope from response")?;
        envelope.source = self.id;
        envelope.stamp();
        envelope
            .ttl()
            .context("response has expired before sending!")?;
        envelope.sign(&self.signer);
        let bytes = envelope
            .write_to_bytes()
            .context("failed to serialize envelope")?;
        log::trace!(
            "pushing outgoing response: {} -> {}",
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
        // start a processor for incoming message
        tokio::spawn(Self::process(self.db.clone(), self.storage.clone(), con));

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
            let msg = match self.storage.messages().await {
                Ok(msg) => msg,
                Err(err) => {
                    log::error!("failed to process local messages: {:#}", err);
                    sleep(wait).await;
                    continue;
                }
            };

            let ret = match msg {
                storage::JsonMessage::Request(request) => self.request(&writer, request).await,
                storage::JsonMessage::Response(response) => self.response(&writer, response).await,
            };

            if let Err(err) = ret {
                log::error!("failed to process message: {}", err);
            }
        }
    }
}
