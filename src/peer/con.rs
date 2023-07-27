use crate::identity::Signer;
use crate::token;
use anyhow::{Context, Result};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{Error, Message};
use url::Url;

const PING_INTERVAL: Duration = Duration::from_secs(20);
const READ_TIMEOUT: Duration = Duration::from_secs(40);

pub struct Connection {
    rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Message>,
}

impl Connection {
    pub async fn read(&mut self) -> Option<Message> {
        self.rx.recv().await
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

impl Connection {
    // creates a retained connection. means it will retry to connect on error .. forever
    // the problem is caller of the system can then only tell if there is a perminent error
    // by checking the logs. this is not very good
    pub fn connect<U: Into<Url>, S>(u: U, id: u32, signer: S) -> Connection
    where
        S: Signer + Send + Sync + 'static,
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
        let connection = Connection {
            rx: down_rx,
            tx: up_tx,
        };

        let u = u.into();
        let builder = token::TokenBuilder::new(id, None, signer);
        tokio::spawn(retainer(u, builder, up_rx, down_tx));
        // we also auto send ping messages to detect stall connections
        let pinger = connection.writer();
        tokio::spawn(async move {
            loop {
                log::debug!("sending a ping");
                if let Err(err) = pinger.write(Message::Ping(Vec::default())).await {
                    log::error!("ping error: {}", err);
                }
                tokio::time::sleep(PING_INTERVAL).await;
            }
        });
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

        let (ws, _) = match tokio_tungstenite::connect_async(&u).await {
            Ok(v) => v,
            Err(err) => {
                log::error!("failed to establish connection: {:#}", err);
                tokio::time::sleep(Duration::from_secs(2)).await;
                log::info!("retrying connection");
                continue;
            }
        };

        let (mut write, read) = ws.split();
        let mut last = Instant::now();

        let mut read = read_stream(read);
        'receive: loop {
            // we check here when was the last time a message was received
            // from the relay. we expect to receive PONG answers (because we
            // send PING every PING_INTERVAL).
            // hence if for some reason there are NO received messaged for
            // period of READ_TIMEOUT, we can safely assume connection is stalling
            // and we can try to reconnect
            if Instant::now().duration_since(last) > READ_TIMEOUT {
                log::debug!("reading timeout trying to reconnect");
                // the problem is on break this message will be lost!
                break 'receive;
            }

            tokio::select! {
                Some(message) = up_rx.recv() => {
                    log::trace!("sending message to relay");
                    if let Err(err) = write.send(message).await {
                        // probably connection closed as well, we need to renew!
                        log::error!("error while sending message: {}", err);
                        break 'receive;
                    }
                },
                message = read.recv() => {
                    let message = match message {
                        None=> {
                            log::debug!("read stream ended")  ;
                            break 'receive;
                        },
                        Some(message) => message,
                    };

                    // we take a note with when a message was received
                    last = Instant::now();
                    log::trace!("received a message from relay");
                    let message = match message {
                        Ok(Message::Pong(_)) => {
                            log::debug!("received a pong");
                            continue 'receive;
                        }
                        Ok(message) => message,
                        Err(err) => {
                            // todo: those errors probably mean we need to re-connect
                            // we will see what to do later.
                            log::error!("error while receiving message: {}", err);
                            break 'receive;
                        }
                    };

                    if let Err(err) = down_tx.send(message).await {
                        log::error!("failed to queue received message for processing: {}", err);
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
        log::info!("retrying connection");
    }
}

fn read_stream(
    mut stream: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) -> mpsc::Receiver<Result<Message, Error>> {
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        loop {
            match stream.next().await {
                None => return,
                Some(result) => {
                    if sender.send(result).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    receiver
}
