mod worker;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use worker::*;

use async_trait::async_trait;

#[async_trait]
pub trait Work {
    async fn run(&self);
}

pub struct WorkerPool<W>
where
    W: Work,
{
    receiver: mpsc::Receiver<oneshot::Sender<W>>,
}

impl<W> WorkerPool<W>
where
    W: Work + Send + Sync + Clone + 'static,
{
    pub async fn new(size: usize) -> WorkerPool<W> {
        let (sender, receiver) = mpsc::channel(1);

        for id in 0..size {
            Worker::new(sender.clone()).await.run().await;
        }

        WorkerPool { receiver }
    }

    pub async fn get(&mut self) -> WorkerHandle<W> {
        let sender = self.receiver.recv().await.unwrap();
        WorkerHandle { sender }
    }
}

pub struct WorkerHandle<W: Work> {
    pub sender: oneshot::Sender<W>,
}

impl<W> WorkerHandle<W>
where
    W: Work,
{
    pub async fn send(self, job: W) -> Result<()> {
        if self.sender.send(job).is_err() {
            bail!("failed to queue job");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{Work, WorkerPool};
    use async_trait::async_trait;
    use tokio::sync::Mutex;

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

    #[tokio::test]
    async fn test_workerpool() {
        let var = Arc::new(Mutex::new(0_u64));
        let adder = Adder {
            var: Arc::clone(&var),
        };
        let mut pool = WorkerPool::<Adder>::new(100).await;

        for _ in 0..=20000 {
            let worker = pool.get().await;
            worker.send(adder.clone()).await;
        }

        let var = *var.lock().await;

        assert_eq!(var, 20000);
    }
}
