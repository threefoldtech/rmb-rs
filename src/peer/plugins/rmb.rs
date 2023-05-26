use tokio::sync::mpsc::Sender;

use crate::types::Envelope;

use super::{Bag, Plugin};

#[derive(Default)]
pub struct Rmb {
    ch: Option<Sender<Bag>>,
}

impl Rmb {
    async fn version(&self, request: &Envelope) {
        log::debug!("got version request");
        let mut response = Envelope {
            uid: request.uid.clone(),
            destination: request.source.clone(),
            expiration: 300,
            schema: Some("application/json".into()),
            ..Default::default()
        };

        response.mut_response();
        response.set_plain(format!("\"{}\"", env!("GIT_VERSION")).as_bytes().into());

        let _ = self.ch.as_ref().unwrap().send(Bag::one(response)).await;
    }
}

#[async_trait::async_trait]
impl Plugin for Rmb {
    fn name(&self) -> &str {
        "rmb"
    }

    async fn remote(&self, incoming: &crate::types::Envelope) {
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
