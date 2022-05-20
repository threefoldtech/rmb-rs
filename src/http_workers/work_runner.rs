use std::sync::Arc;

use async_trait::async_trait;
use hyper::{
    client::{Builder, HttpConnector},
    Body, Client, Method, Request,
};

use crate::{
    cache::Cache,
    twin::{SubstrateTwinDB, Twin, TwinDB},
    types::{Message, QueuedMessage},
    workers::Work,
};

use anyhow::{Context, Result};

#[derive(Clone)]
pub struct WorkRunner<C>
where
    C: Cache<Twin>,
{
    twin_db: SubstrateTwinDB<C>,
}

impl<C> WorkRunner<C>
where
    C: Cache<Twin>,
{
    pub fn new(twin_db: SubstrateTwinDB<C>) -> Self {
        Self { twin_db }
    }
}

#[async_trait]
impl<C> Work for WorkRunner<C>
where
    C: Cache<Twin>,
{
    type Job = QueuedMessage;

    async fn run(&self, job: Self::Job) {
        //identify uri and extract msg
        let (uri_path, msg) = match &job {
            QueuedMessage::Forward(msg) => (String::from("rmb-remote"), msg),
            QueuedMessage::Reply(msg) => (String::from("rmb-reply"), msg),
        };

        // getting twins for all destinations
        // let destinations = &msg.dst;
        // let twins = self.get_twins(destinations).await;

        // prepare requests
        // let requests = Self::build_requests(msg, uri_path, twins).await;

        for id in &msg.dst {
            let twin = self.twin_db.get(id.to_owned() as u32).await;

            let twin = match twin {
                Ok(twin) => twin,
                Err(err) => {
                    log::debug!(
                        "can not retrieve twin from substrate for this id '{}' because of {}",
                        id,
                        err
                    );
                    continue;
                }
            };

            let req = Request::builder()
                .method(Method::POST)
                .header("content-type", "application/json");

            let req = req.uri(format!(
                "{}/{}",
                twin.address.trim_end_matches('/'),
                uri_path
            ));

            let req = req.body(Body::from(serde_json::to_vec(&msg).unwrap()));

            let req = match req {
                Ok(req) => req,
                Err(err) => {
                    log::debug!(
                        "can not construct request body for this id '{}' because of {}",
                        id,
                        err
                    );
                    continue;
                }
            };

            let _response = Client::new().request(req).await;
        }
    }
}
