mod work_runner;
use std::{sync::Arc, time::Duration};

use anyhow::Context;

use crate::{
    cache::Cache,
    storage::Storage,
    twin::{SubstrateTwinDB, Twin, TwinDB},
    types::QueuedMessage,
    workers::WorkerPool,
};

use self::work_runner::WorkRunner;

pub struct HttpWorker<S, C>
where
    S: Storage,
    C: Cache<Twin>,
{
    storage: S,
    pool: WorkerPool<Arc<WorkRunner<C>>>,
}

impl<S, C> HttpWorker<S, C>
where
    S: Storage,
    C: Cache<Twin>,
{
    // this must be async because of the workpool new function must be async
    pub fn new(size: usize, storage: S, twin_db: SubstrateTwinDB<C>) -> Self {
        // let twin_db = SubstrateTwinDB::new("wss://tfchain.dev.grid.tf", cache)
        //     .context("unable to create substrate twin db")?;
        // unwrap because there is no way to return if this not work
        let work_runner = WorkRunner::new(twin_db);
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
