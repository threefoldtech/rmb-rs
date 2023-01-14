use crate::identity::Signer;
use crate::token;
use anyhow::{Context, Result};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

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
        let builder = token::TokenBuilder::new(id, None, signer);
        tokio::spawn(retainer(u, builder, writer_rx, reader_tx));

        connection
    }
}

async fn retainer<S: Signer>(
    mut u: Url,
    b: token::TokenBuilder<S>,
    mut writer_rx: mpsc::Receiver<Message>,
    reader_tx: mpsc::Sender<Message>,
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
