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
