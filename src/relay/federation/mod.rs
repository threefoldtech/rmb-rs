use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, redis::cmd, RedisConnectionManager};
use workers::{Work, WorkerPool};

use self::router::Router;

mod router;

pub const DEFAULT_WORKERS: usize = 100;
pub const FEDERATION_QUEUE: &str = "relay.federation";

pub struct Federation {
    redis_pool: Pool<RedisConnectionManager>,
    workers: usize,
}
impl Federation {
    pub fn new(redis_pool: Pool<RedisConnectionManager>) -> Self {
        Self {
            redis_pool,
            workers: DEFAULT_WORKERS,
        }
    }
}
impl Federation {
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }
    // Sends federation msg to redis on (relay.federation)
    pub async fn send<T: AsRef<[u8]>>(&self, msg: T) -> Result<()> {
        let mut con = self.redis_pool.get().await?;
        cmd("LPUSH")
            .arg(FEDERATION_QUEUE)
            .arg(msg.as_ref())
            .query_async(&mut *con)
            .await?;
        Ok(())
    }

    // start polling from redis and send work to workers
    pub async fn start(&self) {
        let work_runner = Router {};
        let mut worker_pool = WorkerPool::new(Arc::new(work_runner), DEFAULT_WORKERS);
        loop {
            let mut con = match self.redis_pool.get().await {
                Ok(con) => con,
                Err(_) => {
                    log::error!("could not get redis connection from pool");
                    continue;
                }
            };

            let msg: Vec<u8> = match cmd("BRPOP")
                .arg(FEDERATION_QUEUE)
                .query_async(&mut *con)
                .await
            {
                Ok(msg) => msg,
                Err(_) => {
                    log::error!("could not get message from redis");
                    continue;
                }
            };

            let worker_handler = worker_pool.get().await;
            if let Err(err) = worker_handler.send(msg) {
                log::error!("failed to send job to worker: {}", err);
            }
        }
    }
}
