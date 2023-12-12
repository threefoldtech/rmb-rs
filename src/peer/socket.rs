use crate::identity::Signer;
use crate::token;
use anyhow::{Context, Result};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

const PING_INTERVAL: Duration = Duration::from_secs(20);
const READ_TIMEOUT: u64 = 40; // seconds

pub struct Socket {
    rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Message>,
}

impl Socket {
    pub async fn read(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    pub fn writer(&self) -> SocketWriter {
        SocketWriter {
            tx: self.tx.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SocketWriter {
    tx: mpsc::Sender<Message>,
}

impl SocketWriter {
    pub async fn write(&self, message: Message, timeout: Option<Duration>) -> Result<()> {
        if let Some(duration) = timeout {
            self.tx.send_timeout(message, duration).await?;
        } else {
            self.tx.send(message).await?;
        }
        Ok(())
    }
}

impl Socket {
    // creates a retained connection. means it will retry to connect on error .. forever
    // the problem is caller of the system can then only tell if there is a perminent error
    // by checking the logs. this is not very good
    pub fn connect<U: Into<Url>, S>(u: U, id: u32, signer: S) -> Socket
    where
        S: Signer,
    {
        // to support auto reconnect we will run a connection loop in the
        // background. but we will return only reader and writer channels
        // that then can be used to send and receive messages.

        // the "down" channels is attached to the "reader" part of the stream
        // hence it's used to "receiver" or "read" messages
        let (down_tx, down_rx) = mpsc::channel(200);
        // the up channels on the other hand are used to allow writing to the connection (sending to relay)
        let (up_tx, up_rx) = mpsc::channel::<Message>(1);

        // select both futures
        let connection = Socket {
            rx: down_rx,
            tx: up_tx,
        };

        let u = u.into();
        let builder = token::TokenBuilder::new(id, None, signer);
        tokio::spawn(retainer(u, builder, up_rx, down_tx));
        // we also auto send ping messages to detect stall connections

        connection
    }
}

async fn retainer<S: Signer>(
    mut u: Url,
    b: token::TokenBuilder<S>,
    mut up_rx: mpsc::Receiver<Message>,
    down_tx: mpsc::Sender<Message>,
) {
    loop {
        let token = b.token(60).context("failed to create jwt token").unwrap();

        u.set_query(Some(token.as_str()));

        log::info!("connecting to relay {:?}", &u.domain());
        let (ws, _) = match tokio_tungstenite::connect_async(&u).await {
            Ok(v) => v,
            Err(err) => {
                log::error!(
                    "failed to establish connection to {:?} : {:#}",
                    u.domain(),
                    err
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
                log::info!("retrying connection to {:?}", &u.domain());
                continue;
            }
        };
        log::info!("now connected to relay {:?}", &u.domain());

        let (mut write, mut read) = ws.split();
        let ts = Arc::new(AtomicU64::new(timestamp()));

        let down_tx = down_tx.clone();
        let (close, mut closed) = mpsc::channel(1);

        let ts_clone = Arc::clone(&ts);
        let handler = tokio::spawn(async move {
            let ts = ts_clone;
            while let Some(message) = read.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        if close.send(err).await.is_err() {
                            log::error!("failed to notify of socket connection loss");
                        }
                        return;
                    }
                };

                ts.store(timestamp(), Ordering::Relaxed);

                if message.is_pong() {
                    log::debug!("received a pong");
                    continue;
                }

                if let Err(_err) = down_tx.send(message).await {
                    log::error!("failed to push received message");
                }
            }
        });

        'receive: loop {
            tokio::select! {
                read_err = closed.recv() => {
                    if let Some(err) = read_err {
                        log::error!("read error: {}", err);
                    }
                    log::debug!("read routine exited, reconnecting");
                    // force reconnecting
                    break 'receive;
                },
                message = timeout(PING_INTERVAL, up_rx.recv()) => {
                    // first check if we have timed out
                    let now = timestamp();
                    if now - ts.load(Ordering::Relaxed) > READ_TIMEOUT {
                        // we have timed out! we need to reconnect then
                        log::info!("connection timeout! retrying");
                        break 'receive
                    }

                    let message = match message {
                        Ok(Some(message)) => message,
                        Ok(None) => {
                            //weird why would we receive a non message
                            log::error!("socket closed!");
                            handler.abort();
                            return
                        },
                        Err(_) => {
                            // receive timeout (on upstream message)
                            // we can then send a ping to keep the connection alive
                            log::debug!("sending a ping");
                            Message::Ping(Vec::default())
                        }
                    };
                    log::trace!("sending message to relay {:?}", &u.domain());
                    if let Err(err) = write.send(message).await {
                        // probably connection closed as well, we need to renew!
                        log::error!("disconnected: error while sending message: {}", err);
                        break 'receive;
                    }
                }

            }
        }
        // make sure read routine is fully closed
        handler.abort();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
