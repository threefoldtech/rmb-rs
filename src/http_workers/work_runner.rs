use std::{any, fmt::Display, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use chrono::Utc;
use hyper::{
    client::{Builder, HttpConnector},
    Body, Client, Method, Request,
};

use std::convert::TryFrom;

use uriparse::{Authority, Path, Scheme, URIBuilder};

use crate::{
    cache::Cache,
    identity::{Identity, Signer},
    storage::Storage,
    twin::{SubstrateTwinDB, Twin, TwinDB},
    types::{Message, QueuedMessage},
    workers::Work,
};

use anyhow::{Context, Result};

#[derive(PartialEq)]
enum Queue {
    Forward,
    Reply,
}

impl std::convert::AsRef<str> for Queue {
    fn as_ref(&self) -> &str {
        match self {
            Self::Forward => "rmb-remote",
            Self::Reply => "rmb-reply",
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum SendError {
    #[error("receive error reply from remote rmb: `{0}`")]
    Terminal(String),

    #[error("failed to deliver message to remote rmb: {0}")]
    Error(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Signer,
    S: Storage,
{
    twin_db: SubstrateTwinDB<C>,
    identity: I,
    storage: S,
    client: Client<HttpConnector, Body>,
}

impl<C, I, S> WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Signer,
    S: Storage,
{
    pub fn new(
        twin_db: SubstrateTwinDB<C>,
        identity: I,
        storage: S,
        client: Client<HttpConnector, Body>,
    ) -> Self {
        Self {
            twin_db,
            identity,
            storage,
            client,
        }
    }

    async fn get_twin(&self, twin_id: u32, retires: usize) -> Option<Twin> {
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

    fn retries(u: usize) -> usize {
        if u == 0 {
            1
        } else if u > 5 {
            5
        } else {
            u
        }
    }

    fn encrypt_dat(dat: String, twin: &Twin) -> Result<String> {
        Ok(dat)
    }

    fn uri_builder<U: AsRef<str>, A: AsRef<str>>(uri_path: U, twin_address: A) -> Result<String> {
        let mut authority = Authority::try_from(twin_address.as_ref())
            .with_context(|| "can not form authority from twin_address")?;

        if !authority.has_port() {
            authority.set_port(Some(8051));
        }

        let mut uri = URIBuilder::default();
        uri.scheme(Scheme::HTTP)
            .authority(Some(authority))
            .path(Path::try_from(uri_path.as_ref()).with_context(|| "can not form uri path")?);

        let uri = uri
            .build()
            .with_context(|| "can not build the destination uri")?;

        Ok(uri.to_string())
    }

    async fn send_msg<U: AsRef<str>>(
        &self,
        twin: &Twin,
        uri_path: U,
        msg: &Message,
    ) -> Result<(), SendError> {
        let req = Request::builder()
            .method(Method::POST)
            .header("content-type", "application/json");

        let uri = Self::uri_builder(uri_path, &twin.address)?;
        let req = req.uri(uri);

        let req = req
            .body(Body::from(serde_json::to_vec(&msg).unwrap()))
            .with_context(|| format!("can not construct request body for this id '{}'", twin.id))?;

        let response = self
            .client
            .request(req)
            .await
            .context("failure of message delivery!")?;

        // at this point, the remote rmb received our request but then replied with
        // wrong status code (it didn't accept the message for some reason)
        // hence we assume this is a terminal error, and retrying won't fix it.
        if response.status() != http::StatusCode::ACCEPTED {
            Err(SendError::Terminal(format!(
                "received error {} from remote twin",
                response.status()
            )))
        } else {
            Ok(())
        }
    }

    async fn handle_delivery_err(&self, twin: u32, mut msg: Message, err: SendError) {
        let dst = vec![msg.source];
        msg.source = twin;
        msg.destination = dst;
        msg.error = Some(format!("{}", err));
        msg.data = String::default();

        if let Err(err) = self
            .storage
            .reply(&msg)
            .await
            .context("can not send a reply message")
        {
            log::error!("{:?}", err);
        }
    }
}

#[async_trait]
impl<C, I, S> Work for WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Signer,
    S: Storage,
{
    type Job = QueuedMessage;

    async fn run(&self, job: Self::Job) {
        //identify uri and extract msg
        let (queue, msg) = match job {
            QueuedMessage::Forward(msg) => (Queue::Forward, msg),
            QueuedMessage::Reply(msg) => (Queue::Reply, msg),
        };

        let retry = Self::retries(msg.retry);
        for id in &msg.destination {
            let mut msg = msg.clone();
            //let uri_path = uri_path.clone();
            // getting twin object
            let twin = match self.get_twin(*id, retry).await {
                Some(twin) => twin,
                None => {
                    continue;
                }
            };

            // encrypt dat
            msg.data = match Self::encrypt_dat(msg.data, &twin) {
                Ok(dat) => dat,
                Err(err) => {
                    todo!()
                }
            };

            // set time
            msg.now = Utc::now().timestamp() as usize;

            // signing the message
            msg.sign(&self.identity);

            // posting the message to the remote twin
            let mut result = Ok(());
            for _ in 0..retry {
                result = self.send_msg(&twin, &queue, &msg).await;

                if let Err(SendError::Error(err)) = &result {
                    continue;
                }
                break;
            }

            if result.is_err() && queue == Queue::Forward {
                self.handle_delivery_err(twin.id, msg, result.err().unwrap());
            }
        }
    }
}
