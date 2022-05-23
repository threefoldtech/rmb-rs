mod work_runner;
use std::{sync::Arc, time::Duration};

use anyhow::Context;

use crate::{
    cache::Cache,
    identity::Identity,
    storage::Storage,
    twin::{SubstrateTwinDB, Twin, TwinDB},
    types::QueuedMessage,
    workers::WorkerPool,
};

use self::work_runner::WorkRunner;

pub struct HttpWorker<S, C, I>
where
    S: Storage,
    C: Cache<Twin>,
    I: Identity,
{
    storage: S,
    pool: WorkerPool<Arc<WorkRunner<C, I, S>>>,
}

impl<S, C, I> HttpWorker<S, C, I>
where
    S: Storage,
    C: Cache<Twin>,
    I: Identity + 'static,
{
    // this must be async because of the workpool new function must be async
    pub fn new(size: usize, storage: S, twin_db: SubstrateTwinDB<C>, identity: I) -> Self {
        let work_runner = WorkRunner::new(twin_db, identity, storage.clone());
        let pool = WorkerPool::new(Arc::new(work_runner), size);
        Self { storage, pool }
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            loop {
                let worker_handler = self.pool.get().await;

                match self.storage.queued().await {
                    Ok(job) => {
                        worker_handler.send(job);
                    }
                    Err(err) => {
                        log::debug!("error while process the storage because of '{}'", err);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                if let Err(err) = worker_handler.send(job) {
                    log::error!("failed to send job to worker: {}", err);
                }
            }
        });
    }
}
