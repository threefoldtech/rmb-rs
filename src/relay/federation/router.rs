use std::sync::Arc;

use async_trait::async_trait;
use http::{Request, StatusCode};
use hyper::Client;
use workers::{Work, WorkerPool};

use crate::types::{Envelope, EnvelopeExt};
use protobuf::Message;
pub struct Router {}

#[async_trait]
impl Work for Router {
    type Input = Vec<u8>;

    type Output = ();

    async fn run(&self, msg: Self::Input) {
        let envelope = match Envelope::parse_from_bytes(&msg) {
            Ok(envelope) => envelope,
            Err(_) => {
                log::error!("could not parse message");
                return;
            }
        };
        let domain = match envelope.federation {
            Some(domain) => domain,
            None => {
                log::error!("federation information not found in msg");
                return;
            }
        };
        let request = match Request::post(&domain).body(hyper::Body::from(msg)) {
            Ok(request) => request,
            Err(_) => return,
        };
        let client = Client::new();
        let resp = match client.request(request).await {
            Ok(resp) => resp,

            Err(_) => {
                log::error!("could not send request to relay: {}", domain);
                return;
            }
        };
        if resp.status() != StatusCode::OK {
            log::error!(
                "failed to send request to relay: {}, status code: {}",
                domain,
                resp.status()
            );
        }
    }
}

#[tokio::test]
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
    let mut env = Envelope::new();

    env.tags = None;
    env.signature = None;
    env.schema = None;
    env.federation = Some(server.url("/"));
    env.stamp();

    let msg = env.write_to_bytes().unwrap();
    let work_runner = Router {};
    let mut worker_pool = WorkerPool::new(Arc::new(work_runner), 2);
    let worker_handler = worker_pool.get().await;

    worker_handler.run(msg).await.unwrap();
    federation.assert()
}
