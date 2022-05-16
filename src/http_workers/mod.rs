use crate::{storage::Storage, types::QueuedMessage};

use self::worker_pool::WorkerPool;

mod worker;
mod worker_pool;

type Job = Box<dyn FnOnce() + Send + 'static>;

pub enum Msg {
    NewJob(Job),
    Terminate,
}

pub struct HttpWorker<S>
where
    S: Storage,
{
    storage: S,
    pool: WorkerPool,
    available: usize,
}

impl<S> HttpWorker<S>
where
    S: Storage,
{
    pub async fn new(size: usize, storage: S) -> Self {
        let pool = WorkerPool::new(size).await;
        let available = size;
        Self {
            storage,
            pool,
            available,
        }
    }

    pub async fn run(&self) {
        // commented until get the new updates for Storage trait
        /*
        loop {
            if self.available > 0 {
                match Storage::process().await {
                    Ok(queue) => {
                        let job = || {
                            // sign the message
                            // call /rmb-remote or /rmb-reply based on the queue type
                        };
                    }
                    Err(err) => {
                        todo!()
                    }
                }
            }
        }
        */
    }
}
