use crate::types::Envelope;
use anyhow::{Context, Result};
use async_trait::async_trait;
use http::StatusCode;
use protobuf::Message;
use reqwest::Client;
use workers::Work;

pub struct Router {}

impl Router {
    async fn try_send(&self, domain: &str, msg: Vec<u8>) -> Result<()> {
        let url = if cfg!(test) {
            format!("http://{}/", domain)
        } else {
            format!("https://{}/", domain)
        };

        log::debug!("federation to: {}", url);
        let client = Client::new();
        for _ in 0..3 {
            let resp = match client.post(&url).body(msg.clone()).send().await {
                Ok(resp) => resp,
                Err(err) => {
                    if err.is_connect() || err.is_timeout() {
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        continue;
                    }

                    bail!("could not send message to relay: {}", err)
                }
            };

            if resp.status() != StatusCode::ACCEPTED {
                bail!(
                    "received relay did not accept the message: {}",
                    resp.status()
                );
            }

            return Ok(());
        }

        bail!("relay not reachable");
    }

    async fn process(&self, msg: Vec<u8>) -> Result<()> {
        let env = Envelope::parse_from_bytes(&msg).context("failed to parse envelope")?;

        let domain = env
            .federation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("federation is not set on envelope"))?;

        let result = self.try_send(domain, msg).await;
        if result.is_ok() {
            super::MESSAGE_SUCCESS.with_label_values(&[domain]).inc();
        } else {
            super::MESSAGE_ERROR.with_label_values(&[domain]).inc();
            // TODO: send error message back to caller
            // to let him know the message was failed to deliver
        }

        result
    }
}
#[async_trait]
impl Work for Router {
    type Input = Vec<u8>;

    type Output = ();

    async fn run(&self, msg: Self::Input) {
        if let Err(err) = self.process(msg).await {
            log::error!("failed to federation message: {:#}", err);
        }
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::types::{Envelope, EnvelopeExt};
    use std::sync::Arc;
    use workers::WorkerPool;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_router() {
        use httpmock::prelude::*;

        // Start a lightweight mock server.
        let server = MockServer::start();

        // Create a mock on the server.
        let federation = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(202)
                .header("content-type", "text/html")
                .body("ohi");
        });
        let work_runner = Router {};
        let mut worker_pool = WorkerPool::new(Arc::new(work_runner), 2);
        let mut env = Envelope::new();

        env.tags = None;
        env.signature = None;
        env.schema = None;
        env.federation = Some(server.address().to_string());
        env.stamp();

        let worker_handler = worker_pool.get().await;

        worker_handler.send(env.write_to_bytes().unwrap()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        federation.assert()
    }
}
