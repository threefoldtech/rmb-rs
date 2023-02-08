use std::sync::Arc;

use anyhow::Result;
use bb8_redis::{bb8::Pool, redis::cmd, RedisConnectionManager};
use protobuf::Message;
use workers::WorkerPool;

use crate::types::Envelope;

use self::router::Router;

mod router;

pub const DEFAULT_WORKERS: usize = 100;
pub const FEDERATION_QUEUE: &str = "relay.federation";
#[derive(Clone)]
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
        if let Err(err) = cmd("LPUSH")
            .arg(FEDERATION_QUEUE)
            .arg(msg.as_ref())
            .query_async::<_, u32>(&mut *con)
            .await
        {
            bail!("could not push msg to queue: {}", err)
        };
        Ok(())
    }

    // start polling from redis and send work to workers
    pub async fn start(self) {
        let work_runner = Router {};
        let mut worker_pool = WorkerPool::new(Arc::new(work_runner), DEFAULT_WORKERS);
        let mut con = match self.redis_pool.get().await {
            Ok(con) => con,
            Err(err) => {
                panic!("could not get redis connection from pool, {}", err);
            }
        };
        loop {
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
            let env = match Envelope::parse_from_bytes(&msg) {
                Ok(env) => env,
                Err(err) => {
                    log::error!("failed to parse msg to envelop: {}", err);
                    continue;
                }
            };
            if let Err(err) = worker_handler.send(env) {
                log::error!("failed to send job to worker: {}", err);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        redis,
        types::{Envelope, EnvelopeExt},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_router() {
        use httpmock::prelude::*;
        let pool = redis::pool("redis://localhost:6379", 10).await.unwrap();
        let federation_sender = Federation::new(pool.clone()).with_workers(10);
        let federation_receiver = federation_sender.clone();
        tokio::spawn(federation_receiver.start());
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
            env.federation = Some(server.url("/"));
            env.stamp();
            let msg = env.write_to_bytes().unwrap();
            federation_sender.send(msg).await.unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        mock.assert_hits(10);
    }
}
