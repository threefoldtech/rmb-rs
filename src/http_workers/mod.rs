use std::time::Duration;

use crate::{storage::Storage, types::QueuedMessage, workers::WorkerPool};

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
                let worker_handler = self.pool.get().await;

                match self.storage.queued().await {
                    Ok(job) => {
                        worker_handler.send(job).await;
                    }
                    Err(err) => {
                        log::debug!("error while process the storage because of '{}'", err);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }
}
