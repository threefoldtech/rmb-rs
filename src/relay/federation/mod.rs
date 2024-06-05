use std::marker::PhantomData;

use self::router::Router;
use super::{ranker::RelayRanker, switch::Sink};
use crate::twin::TwinDB;
use anyhow::Result;
use bb8_redis::{
    bb8::{Pool, RunError},
    redis::{cmd, RedisError},
    RedisConnectionManager,
};
use prometheus::{IntCounterVec, Opts, Registry};
use workers::WorkerPool;

mod router;

lazy_static::lazy_static! {
    static ref MESSAGE_SUCCESS: IntCounterVec = IntCounterVec::new(
        Opts::new("federation_message_success", "number of messages send successfully via federation"),
        &["relay"]).unwrap();

    static ref MESSAGE_ERROR: IntCounterVec = IntCounterVec::new(
        Opts::new("federation_message_error", "number of messages that failed to send via federation"),
        &["relay"]).unwrap();
}

pub const DEFAULT_WORKERS: usize = 100;
pub const FEDERATION_QUEUE: &str = "relay.federation";

#[derive(thiserror::Error, Debug)]
pub enum FederationError {
    #[error("pool error: {0}")]
    PoolError(#[from] RunError<RedisError>),
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),
}

pub struct FederationOptions<D: TwinDB> {
    pool: Pool<RedisConnectionManager>,
    workers: usize,
    registry: Registry,
    _marker: PhantomData<D>,
}

impl<D> FederationOptions<D>
where
    D: TwinDB + Clone,
{
    pub fn new(pool: Pool<RedisConnectionManager>) -> Self {
        Self {
            pool,
            workers: DEFAULT_WORKERS,
            registry: prometheus::default_registry().clone(),
            _marker: PhantomData,
        }
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    pub(crate) fn build(self, sink: Sink, twins: D, ranker: RelayRanker) -> Result<Federation<D>> {
        Federation::new(self, sink, twins, ranker)
    }
}

pub struct Federation<D: TwinDB> {
    pool: Pool<RedisConnectionManager>,
    workers: WorkerPool<Router<D>>,
}

#[derive(Clone)]
pub struct Federator {
    pool: Pool<RedisConnectionManager>,
}

impl Federator {
    // Sends federation msg to redis on (relay.federation)
    pub async fn send<T: AsRef<[u8]>>(&self, msg: T) -> Result<(), FederationError> {
        let mut con = self.pool.get().await?;

        cmd("LPUSH")
            .arg(FEDERATION_QUEUE)
            .arg(msg.as_ref())
            .query_async::<_, u32>(&mut *con)
            .await?;

        Ok(())
    }
}

impl<D> Federation<D>
where
    D: TwinDB,
{
    /// create a new federation router
    fn new(opts: FederationOptions<D>, sink: Sink, twins: D, ranker: RelayRanker) -> Result<Self> {
        opts.registry.register(Box::new(MESSAGE_SUCCESS.clone()))?;
        opts.registry.register(Box::new(MESSAGE_ERROR.clone()))?;

        let runner = Router::new(sink, twins, ranker);
        let workers = WorkerPool::new(runner, opts.workers);

        Ok(Self {
            pool: opts.pool,
            workers,
        })
    }

    /// start the federation router
    pub fn start(self) -> Federator {
        let federator = Federator {
            pool: self.pool.clone(),
        };

        tokio::spawn(self.run());
        federator
    }

    async fn run(self) {
        let mut workers = self.workers;

        loop {
            let mut con = match self.pool.get().await {
                Ok(con) => con,
                Err(err) => {
                    log::error!("could not get redis connection from pool, {}", err);
                    continue;
                }
            };
            let worker_handler = workers.get().await;
            let (_, msg): (String, Vec<u8>) = match cmd("BRPOP")
                .arg(FEDERATION_QUEUE)
                .arg(0.0)
                .query_async(&mut *con)
                .await
            {
                Ok(msg) => msg,
                Err(err) => {
                    log::error!("could not get message from redis {}", err);
                    continue;
                }
            };
            if let Err(err) = worker_handler.send(msg) {
                log::error!("failed to send job to worker: {}", err);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{
        cache::{Cache, MemCache},
        relay::{ranker::RelayRanker, switch::Sink},
        twin::{RelayDomains, SubstrateTwinDB, Twin},
    };
    use protobuf::Message;
    use subxt::utils::AccountId32;

    use super::*;
    use crate::{
        redis,
        types::{Envelope, EnvelopeExt},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_router() {
        use httpmock::prelude::*;
        let server = MockServer::start();
        let reg = prometheus::Registry::new();
        let pool = redis::pool("redis://localhost:6379", 10).await.unwrap();
        let sink = Sink::new(pool.clone());
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
        let federation = FederationOptions::new(pool)
            .with_registry(reg)
            .with_workers(10)
            .build(sink, db, ranker)
            .unwrap();

        let federator = federation.start();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(202)
                .header("content-type", "text/html")
                .body("ohi");
        });

        for _ in 0..10 {
            let mut env = Envelope::new();
            env.tags = None;
            env.signature = None;
            env.schema = None;
            env.destination = Some(twin_id.into()).into();
            env.stamp();
            let msg = env.write_to_bytes().unwrap();
            federator.send(msg).await.unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        mock.assert_hits(10);
    }
}
