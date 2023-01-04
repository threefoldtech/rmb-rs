use crate::identity::Signer;
use crate::types::{Envelope, EnvelopeExt, JsonRequest, JsonResponse};
use anyhow::{Context, Result};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use protobuf::Message as ProtoMessage;
use std::time::Duration;
use storage::Storage;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

pub mod storage;

const MIN_RETRIES: usize = 1;
const MAX_RETRIES: usize = 5;

pub struct Peer<S: Storage, G: Signer> {
    id: u32,
    storage: S,
    signer: G,
    con: Connection,
}

impl<S, G> Peer<S, G>
where
    S: Storage,
    G: Signer + Send + Sync + 'static,
{
    pub async fn new(rely: Url, id: u32, storage: S, signer: G) -> Self {
        let con = connect(rely).await;
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
                    log::error!("failed to process local messages: {}", err);
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

pub struct Connection {
    rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Message>,
}

impl Connection {
    pub async fn read(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    pub async fn write(&self, message: Message) -> Result<()> {
        self.tx.send(message).await?;
        Ok(())
    }

    pub fn writer(&self) -> Writer {
        Writer {
            tx: self.tx.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Writer {
    tx: mpsc::Sender<Message>,
}

impl Writer {
    pub async fn write(&self, message: Message) -> Result<()> {
        self.tx.send(message).await?;
        Ok(())
    }
}

// creates a retained connection. means it will retry to connect on error .. forever
// the problem is caller of the system can then only tell if there is a perminent error
// by checking the logs. this is not very good
pub async fn connect<U: Into<Url>>(u: U) -> Connection {
    // to support auto reconnect we will run a connection loop in the
    // background. but we will return only reader and writer channels
    // that then can be used to send and receive messages.

    // the "reader" channels is attached to the "reader" part of the stream
    // hence it's used to "receiver" or "read" messages
    let (reader_tx, reader_rx) = mpsc::channel(200);
    // the writer channels on the other hand are used to allow writing to the connection
    let (writer_tx, writer_rx) = mpsc::channel::<Message>(1);

    // select both futures
    let connection = Connection {
        rx: reader_rx,
        tx: writer_tx,
    };

    let u = u.into();
    tokio::spawn(retainer(u, writer_rx, reader_tx));

    connection
}

async fn retainer(
    u: Url,
    mut writer_rx: mpsc::Receiver<Message>,
    reader_tx: mpsc::Sender<Message>,
) {
    loop {
        let (ws, _) = match tokio_tungstenite::connect_async(&u).await {
            Ok(v) => v,
            Err(err) => {
                log::error!("failed to establish connection: {}", err);
                tokio::time::sleep(Duration::from_secs(2)).await;
                log::info!("retrying connection");
                continue;
            }
        };

        let (mut write, mut read) = ws.split();
        'receive: loop {
            tokio::select! {
                Some(message) = writer_rx.recv() => {
                    if let Err(err) = write.send(message).await {
                        // probably connection closed as well, we need to renew!
                        log::error!("error while sending message: {}", err);
                        break 'receive;
                    }
                },
                Some(message) = read.next() => {
                    let message = match message {
                        Ok(message) => message,
                        Err(err) => {
                            // todo: those errors probably mean we need to re-connect
                            // we will see what to do later.
                            log::error!("error while receiving message: {}", err);
                            break 'receive;
                        }
                    };

                    if let Err(err) = reader_tx.send(message).await {
                        log::error!("failed to queue received message for processing: {}", err);
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
        log::info!("retrying connection");
    }
}

fn between<T: Ord>(v: T, min: T, max: T) -> T {
    if v < min {
        return min;
    } else if v > max {
        return max;
    }

    v
}
