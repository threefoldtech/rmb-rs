use crate::{
    relay::switch::{Sink, StreamID},
    types::Envelope,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use http::StatusCode;
use protobuf::Message;
use reqwest::Client;
use workers::Work;

#[derive(Clone)]
pub(crate) struct Router {
    sink: Option<Sink>,
}

impl Router {
    pub fn new(sink: Sink) -> Self {
        Self { sink: Some(sink) }
    }

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

                    bail!("could not send message to relay '{}': {}", domain, err)
                }
            };

            if resp.status() != StatusCode::ACCEPTED {
                bail!(
                    "received relay '{}' did not accept the message: {}",
                    domain,
                    resp.status()
                );
            }

            return Ok(());
        }

        bail!("relay '{}' was not reachable in time", domain);
    }

    async fn process(&self, msg: Vec<u8>) -> Result<()> {
        let env = Envelope::parse_from_bytes(&msg).context("failed to parse envelope")?;

        let domain = env
            .federation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("federation is not set on envelope"))?;

        let result = self.try_send(domain, msg).await;
        match result {
            Ok(_) => super::MESSAGE_SUCCESS.with_label_values(&[domain]).inc(),
            Err(ref err) => {
                super::MESSAGE_ERROR.with_label_values(&[domain]).inc();

                if let Some(ref sink) = self.sink {
                    let mut msg = Envelope::new();
                    msg.uid = env.uid;
                    let e = msg.mut_error();
                    e.message = err.to_string();
                    let dst: StreamID = (&env.source).into();

                    let _ = sink.send(&dst, msg.write_to_bytes()?).await;
                    // after this point we don't care if the error was not reported back
                }
            }
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
            log::debug!("failed to federation message: {:#}", err);
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
        let work_runner = Router { sink: None };
        let mut worker_pool = WorkerPool::new(Arc::new(work_runner), 2);
        let mut env = Envelope::new();

        env.tags = None;
        env.signature = None;
        env.schema = None;
        env.federation = Some(server.address().to_string());
        env.stamp();

        let worker_handler = worker_pool.get().await;

        worker_handler.send(env.write_to_bytes().unwrap()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        federation.assert()
    }
}
