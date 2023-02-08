use async_trait::async_trait;
use http::StatusCode;
use protobuf::Message;
use workers::Work;

use crate::types::Envelope;

pub struct Router {}

#[async_trait]
impl Work for Router {
    type Input = Envelope;

    type Output = ();

    async fn run(&self, env: Self::Input) {
        let domain = match &env.federation {
            Some(domain) => domain,
            None => {
                log::error!("federation information not found in msg");
                return;
            }
        };
        let msg = match env.write_to_bytes() {
            Ok(msg) => msg,
            Err(err) => {
                log::error!("could not decode envelop, {}", err);
                return;
            }
        };
        let url = format!("https://{}/", domain);
        let client = reqwest::Client::new();
        let resp = match client.post(&url).body(msg).send().await {
            Ok(resp) => resp,

            Err(_) => {
                log::error!("could not send request to relay: {}", &url);
                return;
            }
        };
        if resp.status() != StatusCode::OK && resp.status() != StatusCode::ACCEPTED {
            log::error!(
                "failed to send request to relay: {}, status code: {}",
                url,
                resp.status()
            );
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
            then.status(200)
                .header("content-type", "text/html")
                .body("ohi");
        });
        let work_runner = Router {};
        let mut worker_pool = WorkerPool::new(Arc::new(work_runner), 2);
        let mut env = Envelope::new();

        env.tags = None;
        env.signature = None;
        env.schema = None;
        env.federation = Some(server.url("/"));
        env.stamp();

        let worker_handler = worker_pool.get().await;

        worker_handler.send(env).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        federation.assert()
    }
}
