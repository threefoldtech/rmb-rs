mod worker;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use worker::*;

use async_trait::async_trait;

#[async_trait]
pub trait Work {
    type Job: Send + 'static;
    async fn run(&self, job: Self::Job);
}

pub struct WorkerPool<W>
where
    W: Work,
{
    receiver: mpsc::Receiver<oneshot::Sender<W::Job>>,
}

impl<W> WorkerPool<W>
where
    W: Work + Send + Sync + Clone + 'static,
{
    // this must be async because the run function is async
    pub async fn new(work: W, size: usize) -> WorkerPool<W> {
        let (sender, receiver) = mpsc::channel(1);

        for id in 0..size {
            Worker::new(work.clone(), sender.clone()).run().await;
        }

        WorkerPool { receiver }
    }

    pub async fn get(&mut self) -> WorkerHandle<W> {
        let sender = self.receiver.recv().await.unwrap();
        WorkerHandle { sender }
    }
}

pub struct WorkerHandle<W: Work> {
    pub sender: oneshot::Sender<W::Job>,
}

impl<W> WorkerHandle<W>
where
    W: Work,
{
    pub fn send(self, job: W::Job) -> Result<()> {
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
        pub inc_val: u64,
    }

    #[async_trait]
    impl Work for Adder {
        type Job = Arc<Mutex<u64>>;
        async fn run(&self, job: Self::Job) {
            let mut var = job.lock().await;
            *var += self.inc_val;
        }
    }

    #[tokio::test]
    async fn test_workerpool() {
        let var = Arc::new(Mutex::new(0_u64));
        let adder = Adder { inc_val: 1_u64 };
        let mut pool = WorkerPool::<Adder>::new(adder, 100).await;

        for _ in 0..=20000 {
            let worker = pool.get().await;
            worker.send(Arc::clone(&var));
        }

        let var = *var.lock().await;

        assert_eq!(var, 20000);
    }
}
