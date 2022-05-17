use crate::{storage::Storage, types::QueuedMessage};

use self::worker_pool::WorkerPool;
use async_trait::async_trait;
mod worker;
mod worker_pool;

#[async_trait]
pub trait Work {
    async fn run(&self);
}

pub struct HttpWorker<S>
where
    S: Storage,
{
    storage: S,
    pool: WorkerPool<QueuedMessage>,
}

impl<S> HttpWorker<S>
where
    S: Storage + 'static,
{
    pub async fn new(size: usize, storage: S) -> Self {
        let pool = WorkerPool::new(size).await;
        Self { storage, pool }
    }

    pub async fn run(mut self) {
        tokio::spawn(async move {
            loop {
                let worker_handler = self.pool.recv().await;

                match self.storage.process().await {
                    Ok(job) => {
                        worker_handler.send(job).await;
                    }
                    Err(_) => todo!(),
                }
            }
        });
    }
}
