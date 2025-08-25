use std::fmt::Debug;
use std::time::Duration;

use crate::{
    relay::build_error_envelope,
    relay::ranker::RelayRanker,
    relay::switch::{SessionID, Sink},
    twin::TwinDB,
    types::{Envelope, EnvelopeExt},
};
// anyhow::Context no longer used after error typing refactor
use async_trait::async_trait;
use http::StatusCode;
use protobuf::Message;
use rand::{thread_rng, Rng};
use reqwest::Client;
use tokio::time::sleep;
use workers::Work;

#[derive(Clone)]
pub(crate) struct Router<D: TwinDB> {
    sink: Option<Sink>,
    twins: D,
    ranker: RelayRanker,
    client: Client,
}

#[derive(thiserror::Error, Debug)]
pub enum RouterError {
    #[error("remote relay rejected permanently: {0}")]
    Permanent(String),
    #[error("remote relay failed temporarily: {0}")]
    Transient(String),
    #[error("message has expired")]
    Expired,
}

impl<D> Router<D>
where
    D: TwinDB,
{
    pub fn new(sink: Sink, twins: D, ranker: RelayRanker) -> Self {
        // Configure HTTP client with production-friendly defaults
        let client = Client::builder()
            // Avoid your application hanging indefinitely
            .connect_timeout(Duration::from_secs(2))
            // Overall per-request ceiling
            .timeout(Duration::from_secs(12))
            // This keeps a connection in the connection pool for up to 5 minutes
            // drastically reduce overhead by reusing existing connections for subsequent requests
            .pool_idle_timeout(Duration::from_secs(300))
            // Help detect dead paths sooner
            .tcp_keepalive(Duration::from_secs(60))
            // Let reqwest/h2 auto-tune flow control when available
            .http2_adaptive_window(true)
            .build()
            .expect("failed to build reqwest client");

        Self {
            sink: Some(sink),
            twins,
            ranker,
            client,
        }
    }

    async fn try_send<'a, S: AsRef<str> + Debug>(
        &self,
        domains: &'a Vec<S>,
        msg: Vec<u8>,
    ) -> Result<&'a str, RouterError> {
        // Up to 3 passes over candidate relays with small jittered backoff between passes
        for pass in 0..3 {
            for domain in domains.iter() {
                let url = if cfg!(test) {
                    format!("http://{}/", domain.as_ref())
                } else {
                    format!("https://{}/", domain.as_ref())
                };
                log::debug!("federation to: {}", url);
                let resp = match self
                    .client
                    .post(&url)
                    .timeout(Duration::from_secs(3))
                    .body(msg.clone())
                    .send()
                    .await
                {
                    Ok(resp) => resp,
                    Err(err) => {
                        log::warn!(
                            "could not send message to relay '{}': {}",
                            domain.as_ref(),
                            err
                        );
                        self.ranker.downvote(domain.as_ref()).await;
                        continue;
                    }
                };

                if resp.status() == StatusCode::ACCEPTED {
                    return Ok(domain.as_ref());
                }

                // Treat client errors (4xx) as non-retryable: return immediately with error
                if resp.status().is_client_error() {
                    log::warn!(
                        "relay '{}' rejected the message with client error: {} (non-retryable)",
                        domain.as_ref(),
                        resp.status()
                    );
                    self.ranker.downvote(domain.as_ref()).await;
                    return Err(RouterError::Permanent(resp.status().to_string()));
                }

                // For other statuses (e.g., 5xx), downvote and try next candidate
                log::warn!(
                    "received relay '{}' did not accept the message: {}",
                    domain.as_ref(),
                    resp.status()
                );
                self.ranker.downvote(domain.as_ref()).await;
                continue;
            }

            // Jittered exponential backoff between passes except after the last pass
            if pass < 2 {
                let base_ms = 100u64 << pass; // 100ms, 200ms
                let jitter: u64 = thread_rng().gen_range(0..(base_ms / 2 + 1));
                sleep(Duration::from_millis(base_ms + jitter)).await;
            }
        }
        Err(RouterError::Transient(format!(
            "relays '{:?}' was not reachable in time",
            domains
        )))
    }

    pub(crate) async fn process(&self, msg: Vec<u8>) -> Result<(), RouterError> {
        let env = Envelope::parse_from_bytes(&msg)
            .map_err(|e| RouterError::Permanent(format!("failed to parse envelope: {}", e)))?;
        // TTL check early
        if env.ttl().map(|d| d.as_secs()).unwrap_or(0) == 0 {
            return Err(RouterError::Expired);
        }
        let twin = self
            .twins
            .get_twin(env.destination.twin.into())
            .await
            .map_err(|e| RouterError::Transient(format!("failed to get twin details: {}", e)))?
            .ok_or_else(|| RouterError::Permanent("self twin not found!".into()))?;
        let domains = twin
            .relay
            .ok_or_else(|| RouterError::Permanent("relay is not set for this twin".into()))?;
        let mut sorted_doamin = domains.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
        self.ranker.reorder(&mut sorted_doamin).await;
        let result = self.try_send(&sorted_doamin, msg).await;
        match result {
            Ok(domain) => {
                super::MESSAGE_SUCCESS.with_label_values(&[domain]).inc();
                Ok(())
            }
            Err(err) => {
                for d in domains.iter() {
                    super::MESSAGE_ERROR.with_label_values(&[d]).inc();
                }
                // Do not send error response here; worker decides based on error kind
                Err(err)
            }
        }
    }

    // Public helper used by retry path to send an error response back to requester
    pub async fn send_error_response(&self, env: &Envelope, err: &str) {
        if env.has_request() {
            if let Some(ref sink) = self.sink {
                let msg = build_error_envelope(env, err);
                let dst: SessionID = (&env.source).into();

                match msg.write_to_bytes() {
                    Ok(bytes) => {
                        if let Err(err) = sink.send(&dst, bytes).await {
                            // just log then
                            log::error!("failed to send error message back to caller: {}", err);
                        }
                    }
                    Err(e) => {
                        log::error!("failed to serialize error envelope: {}", e);
                    }
                }
            }
        }
    }
}

#[async_trait]
impl<D> Work for Router<D>
where
    D: TwinDB,
{
    type Input = Vec<u8>;

    type Output = ();

    async fn run(&mut self, msg: Self::Input) {
        if let Err(err) = self.process(msg).await {
            log::debug!("failed to federation message: {:#}", err);
        }
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        cache::{Cache, MemCache},
        twin::{RelayDomains, SubstrateTwinDB, Twin},
        types::{Envelope, EnvelopeExt},
    };
    use std::time::Duration;
    use subxt::utils::AccountId32;
    use workers::WorkerPool;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_router() {
        use httpmock::prelude::*;

        // Start a lightweight mock server.
        let server = MockServer::start();
        let mem: MemCache<Twin> = MemCache::default();
        let account_id: AccountId32 = "5EyHmbLydxX7hXTX7gQqftCJr2e57Z3VNtgd6uxJzZsAjcPb"
            .parse()
            .unwrap();
        let twin_id = 1;
        let twin = Twin {
            id: twin_id.into(),
            account: account_id,
            relay: Some(RelayDomains::new(&[server.address().to_string()])),
            pk: None,
        };
        let _ = mem.set(1, twin.clone()).await;
        let db = SubstrateTwinDB::new(
            vec![String::from("wss://tfchain.dev.grid.tf:443")],
            Some(mem.clone()),
        )
        .await
        .unwrap();
        let ranker = RelayRanker::new(Duration::from_secs(3600));
        // Create a mock on the server.
        let federation = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(202)
                .header("content-type", "text/html")
                .body("ohi");
        });
        let work_runner = Router {
            sink: None,
            twins: db,
            ranker,
            client: Client::new(),
        };
        let mut worker_pool = WorkerPool::new(work_runner, 2);
        let mut env = Envelope::new();

        env.tags = None;
        env.signature = None;
        env.schema = None;
        env.destination = Some(twin_id.into()).into();
        // Ensure TTL is non-zero; Router::process() rejects expired messages
        env.expiration = 60;
        env.stamp();

        let worker_handler = worker_pool.get().await;

        worker_handler.send(env.write_to_bytes().unwrap()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        federation.assert()
    }
}
