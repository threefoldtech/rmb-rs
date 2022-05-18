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

                match self.storage.process().await {
                    Ok(job) => {
                        worker_handler.send(job).await;
                    }
                    Err(err) => {
                        log::debug!("Error while process the storage because of '{}'", err);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::{
        cache::MemCache,
        types::Message,
        workers::{Work, WorkerPool},
    };

    #[derive(Clone)]
    struct Adder {
        pub var: Arc<Mutex<u64>>,
    }

    #[async_trait]
    impl Work for Adder {
        async fn run(&self) {
            let mut var = self.var.lock().await;
            *var += 1;
        }
    }

    #[derive(Clone)]
    struct MemStorage {
        pub adder: Adder,
    }

    #[async_trait]
    impl Storage for MemStorage {
        async fn set(_msg: Message) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn get(_id: String) -> anyhow::Result<Option<Message>> {
            unimplemented!()
        }

        async fn run(_msg: Message) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn forward(_msg: Message) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn reply(_msg: Message) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn local() -> anyhow::Result<Message> {
            unimplemented!()
        }

        async fn process(&self) -> anyhow::Result<crate::types::QueuedMessage> {
            self.adder.run().await;
            Ok(QueuedMessage::Forward(Message::default()))
        }
    }

    #[tokio::test]
    async fn test_http_workers() {
        let var = Arc::new(Mutex::new(0_u64));
        let adder = Adder {
            var: Arc::clone(&var),
        };

        let http_worker = HttpWorker::new(100, MemStorage { adder }).await;
        http_worker.run().await;

        let var = *var.lock().await;

        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(var > 0);
    }
}
