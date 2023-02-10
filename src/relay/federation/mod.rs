use anyhow::Result;
use bb8_redis::{
    bb8::{Pool, RunError},
    redis::{cmd, RedisError},
    RedisConnectionManager,
};
use prometheus::{IntCounterVec, Opts, Registry};
use std::sync::Arc;
use workers::WorkerPool;

use self::router::Router;

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

pub struct FederationOptions {
    pool: Pool<RedisConnectionManager>,
    workers: usize,
    registry: Registry,
}

impl FederationOptions {
    pub fn new(pool: Pool<RedisConnectionManager>) -> Self {
        Self {
            pool,
            workers: DEFAULT_WORKERS,
            registry: prometheus::default_registry().clone(),
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

    pub fn build(self) -> Result<Federation> {
        Federation::new(self)
    }
}

pub struct Federation {
    pool: Pool<RedisConnectionManager>,
    workers: usize,
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

impl Federation {
    /// create a new federation router
    fn new(opts: FederationOptions) -> Result<Self> {
        opts.registry.register(Box::new(MESSAGE_SUCCESS.clone()))?;
        opts.registry.register(Box::new(MESSAGE_SUCCESS.clone()))?;

        Ok(Self {
            pool: opts.pool,
            workers: opts.workers,
        })
    }

    /// set number of federation workers
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
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
        let work_runner = Router {};
        let mut worker_pool = WorkerPool::new(Arc::new(work_runner), DEFAULT_WORKERS);

        loop {
            let mut con = match self.pool.get().await {
                Ok(con) => con,
                Err(err) => {
                    log::error!("could not get redis connection from pool, {}", err);
                    continue;
                }
            };
            let worker_handler = worker_pool.get().await;
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
    use protobuf::Message;

    use super::*;
    use crate::{
        redis,
        types::{Envelope, EnvelopeExt},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_router() {
        use httpmock::prelude::*;
        let pool = redis::pool("redis://localhost:6379", 10).await.unwrap();

        let federation = FederationOptions::new(pool.clone())
            .with_workers(10)
            .build()
            .unwrap();

        let federator = federation.start();

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                .header("content-type", "text/html")
                .body("ohi");
        });
        for _ in 0..10 {
            let mut env = Envelope::new();
            env.tags = None;
            env.signature = None;
            env.schema = None;
            env.federation = Some(server.address().to_string());
            env.stamp();
            let msg = env.write_to_bytes().unwrap();
            federator.send(msg).await.unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        mock.assert_hits(10);
    }
}
