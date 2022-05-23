use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use chrono::Utc;
use hyper::{
    client::{Builder, HttpConnector},
    Body, Client, Method, Request,
};

use crate::{
    cache::Cache,
    identity::Identity,
    storage::Storage,
    twin::{SubstrateTwinDB, Twin, TwinDB},
    types::{Message, QueuedMessage},
    workers::Work,
};

use anyhow::{Context, Result};

#[derive(Clone)]
pub struct WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Identity,
    S: Storage,
{
    twin_db: SubstrateTwinDB<C>,
    identity: I,
    storage: S,
}

impl<C, I, S> WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Identity,
    S: Storage,
{
    pub fn new(twin_db: SubstrateTwinDB<C>, identity: I, storage: S) -> Self {
        Self {
            twin_db,
            identity,
            storage,
        }
    }

    async fn get_twin(&self, twin_id: usize, retires: usize) -> Option<Twin> {
        for _ in 0..retires {
            let twin = self.twin_db.get_twin(twin_id.to_owned() as u32).await;

            match twin {
                Ok(twin) => {
                    return twin;
                }
                Err(err) => {
                    log::debug!(
                        "can not retrieve twin from substrate for this id '{}' because of {}",
                        twin_id,
                        err
                    );
                }
            };
        }
        None
    }

    fn encrypt_dat(dat: String, pubkey: String) -> Result<String> {
        todo!()
    }

    async fn send_msg(twin: Twin, uri_path: String, msg: Message, dst: usize) -> Result<()> {
        let req = Request::builder()
            .method(Method::POST)
            .header("content-type", "application/json");

        let req = req.uri(format!(
            "{}/{}",
            twin.address.trim_end_matches('/'),
            uri_path
        ));

        let req = req
            .body(Body::from(serde_json::to_vec(&msg).unwrap()))
            .context(format!(
                "can not construct request body for this id '{}'",
                dst
            ))?;

        let req = Request::from(req);
        Client::new()
            .request(req)
            .await
            .map(|op| ())
            .context("failure of message delivery!")
    }

    async fn handle_delivery_err(&self, is_forward: bool, src: usize, err: anyhow::Error) {
        if is_forward {
            let mut reply_msg = Message::default();
            reply_msg.src = src;
            reply_msg.dst = vec![src];
            reply_msg.err = Some(err.to_string());

            if let Err(err) = self
                .storage
                .reply(reply_msg)
                .await
                .context("can not send a reply message")
            {
                log::info!("{:?}", err);
            }
        } else {
            log::info!("{:?}", err);
        }
    }
}

#[async_trait]
impl<C, I, S> Work for WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Identity,
    S: Storage,
{
    type Job = QueuedMessage;

    async fn run(&self, job: Self::Job) {
        //identify uri and extract msg
        let (uri_path, msg) = match job {
            QueuedMessage::Forward(msg) => (String::from("rmb-remote"), msg),
            QueuedMessage::Reply(msg) => (String::from("rmb-reply"), msg),
        };

        for id in &msg.dst {
            let mut msg = msg.clone();
            let uri_path = uri_path.clone();
            // getting twin object
            let twin = match self.get_twin(id.to_owned(), msg.retry).await {
                Some(twin) => twin,
                None => {
                    continue;
                }
            };

            // encrypt dat
            msg.dat = match Self::encrypt_dat(msg.dat.clone(), twin.account.to_string()) {
                Ok(dat) => dat,
                Err(err) => {
                    todo!()
                }
            };

            // set time
            msg.now = Utc::now().timestamp() as usize;

            // signing the message
            let msg = match self.identity.sign(msg) {
                Ok(msg) => msg,
                Err(err) => todo!(),
            };

            // posting the message to the remote twin
            let mut send_result = Ok(());
            for _ in 0..msg.retry {
                send_result =
                    Self::send_msg(twin.clone(), uri_path.clone(), msg.clone(), id.to_owned())
                        .await;
                if send_result.is_ok() {
                    break;
                }
            }

            // handling delivery errors
            if send_result.is_err() {
                self.handle_delivery_err(
                    uri_path.ends_with("remote"),
                    msg.src.clone(),
                    send_result.err().unwrap(),
                );
            }
        }
    }
}
