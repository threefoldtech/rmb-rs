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
    pub async fn new(cache: Option<C>, twin_db: SubstrateTwinDB<C>) -> Result<Self> {
        Ok(Self { twin_db })
    }

    async fn get_twins(&self, dest: &Vec<usize>) -> Vec<Twin> {
        let mut twins = vec![];
        for dst in dest.iter() {
            let twin = self.twin_db.get(dst.to_owned() as u32).await;
            match twin {
                Ok(twin) => twins.push(twin),
                Err(err) => {
                    log::debug!("can't get twin from substrate because of '{}'", err);
                }
            };
        }

        return twins;
    }

    async fn build_requests(
        msg: &Message,
        uri_path: String,
        twins: Vec<Twin>,
    ) -> Vec<Request<Body>> {
        let mut reqs = vec![];
        for twin in twins {
            let req = Request::builder()
                .method(Method::POST)
                .header("content-type", "application/json");

            let req = req.uri(format!(
                "{}/{}",
                twin.address.trim_end_matches("/"),
                uri_path
            ));

            let req = req.body(Body::from(serde_json::to_vec(&msg).unwrap()));
            match req {
                Ok(req) => reqs.push(req),
                Err(err) => {
                    log::debug!("can not build the work runner request because of '{}'", err);
                }
            };
        }

        reqs
    }

    async fn get_responses(requests: Vec<Request<Body>>) {
        let client = Client::new();
        for req in requests {
            let _resp = client.request(req).await;
        }
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
        let destinations = &msg.dst;
        let twins = self.get_twins(destinations).await;

        // prepare requests
        let requests = Self::build_requests(msg, uri_path, twins).await;

        // get responses
        let _responses = Self::get_responses(requests).await;
    }
}
