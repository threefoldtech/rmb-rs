use std::fmt::Display;

use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::{Bag, Plugin};
use crate::types::{Backlog, Envelope};
use anyhow::{Context, Result};

#[derive(Default)]
pub struct Rmb {
    ch: Option<Sender<Bag>>,
}

impl Rmb {
    async fn version(&self, request: &Envelope) {
        log::trace!("got version request");

        let response = match self.response(request, Ok::<_, &str>(env!("GIT_VERSION"))) {
            Ok(response) => response,
            Err(err) => {
                log::error!("failed to create a response message: {}", err);
                return;
            }
        };

        let _ = self.ch.as_ref().unwrap().send(Bag::one(response)).await;
    }

    fn response<O: Serialize, E: Display>(
        &self,
        request: &Envelope,
        msg: Result<O, E>,
    ) -> Result<Envelope> {
        let mut response = Envelope {
            uid: request.uid.clone(),
            destination: request.source.clone(),
            expiration: 300,
            schema: Some("application/json".into()),
            ..Default::default()
        };

        match msg {
            Ok(data) => {
                response.mut_response();
                let bytes = serde_json::to_vec(&data).context("failed to serialize response")?;

                response.set_plain(bytes);
            }
            Err(err) => {
                let e = response.mut_error();
                e.message = err.to_string();
            }
        };

        Ok(response)
    }
}

#[async_trait::async_trait]
impl Plugin for Rmb {
    fn name(&self) -> &str {
        "rmb"
    }

    async fn remote(&self, _: Option<Backlog>, incoming: &crate::types::Envelope) {
        log::debug!("got rmb plugin request");
        if !incoming.has_request() {
            return;
        }
        let req = incoming.request();
        match req.command.as_str() {
            "rmb.version" => self.version(incoming).await,
            _ => {
                log::error!("unknown built in command: {}", req.command);
            }
        }
    }

    fn start(&mut self, sender: Sender<Bag>) {
        self.ch = Some(sender);
    }
}
