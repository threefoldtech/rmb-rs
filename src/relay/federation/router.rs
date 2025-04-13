use std::fmt::Debug;

use crate::{
    relay::ranker::RelayRanker,
    relay::switch::{SessionID, Sink},
    twin::TwinDB,
    types::{Envelope, EnvelopeExt},
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use http::StatusCode;
use protobuf::Message;
use reqwest::Client;
use workers::Work;

#[derive(Clone)]
pub(crate) struct Router<D: TwinDB> {
    sink: Option<Sink>,
    twins: D,
    ranker: RelayRanker,
    client: Client,
}

impl<D> Router<D>
where
    D: TwinDB,
{
    pub fn new(sink: Sink, twins: D, ranker: RelayRanker) -> Self {
        Self {
            sink: Some(sink),
            twins,
            ranker,
            client: Client::new(),
        }
    }

    async fn try_send<'a, S: AsRef<str> + Debug>(
        &self,
        domains: &'a Vec<S>,
        msg: Vec<u8>,
    ) -> Result<&'a str> {
        // TODO: FIX ME
        for _ in 0..3 {
            for domain in domains.iter() {
                let url = if cfg!(test) {
                    format!("http://{}/", domain.as_ref())
                } else {
                    format!("https://{}/", domain.as_ref())
                };
                log::debug!("federation to: {}", url);
                let resp = match self.client.post(&url).body(msg.clone()).send().await {
                    Ok(resp) => resp,
                    Err(err) => {
                        log::warn!(
                            "could not send message to relay '{}': {}",
                            domain.as_ref(),
                            err
                        );
                        self.ranker.downvote(domain.as_ref()).await;
                        continue;
                        // bail!("could not send message to relay '{}': {}", domain, err)
                    }
                };

                if resp.status() != StatusCode::ACCEPTED {
                    log::warn!(
                        "received relay '{}' did not accept the message: {}",
                        domain.as_ref(),
                        resp.status()
                    );
                    self.ranker.downvote(domain.as_ref()).await;
                    continue;
                }

                return Ok(domain.as_ref());
            }
        }
        bail!("relays '{:?}' was not reachable in time", domains);
    }

    async fn process(&self, msg: Vec<u8>) -> Result<()> {
        let env = Envelope::parse_from_bytes(&msg).context("failed to parse envelope")?;
        let twin = self
            .twins
            .get_twin(env.destination.twin)
            .await
            .context("failed to get twin details")?
            .ok_or_else(|| anyhow::anyhow!("self twin not found!"))?;
        let domains = twin
            .relay
            .ok_or_else(|| anyhow::anyhow!("relay is not set for this twin"))?;
        let mut sorted_doamin = domains.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
        self.ranker.reorder(&mut sorted_doamin).await;
        let result = self.try_send(&sorted_doamin, msg).await;
        match result {
            Ok(domain) => super::MESSAGE_SUCCESS.with_label_values(&[domain]).inc(),
            Err(ref err) => {
                for d in domains.iter() {
                    super::MESSAGE_ERROR.with_label_values(&[d]).inc();
                }

                if let Some(ref sink) = self.sink {
                    let mut msg = Envelope::new();
                    msg.expiration = 300;
                    msg.stamp();
                    msg.uid = env.uid;
                    let e = msg.mut_error();
                    e.message = err.to_string();
                    let dst: SessionID = (&env.source).into();

                    let _ = sink.send(&dst, msg.write_to_bytes()?).await;
                    // after this point we don't care if the error was not reported back
                }
            }
        }

        result.map(|_| ())
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
            id: twin_id,
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
        env.stamp();

        let worker_handler = worker_pool.get().await;

        worker_handler.send(env.write_to_bytes().unwrap()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        federation.assert()
    }
}
