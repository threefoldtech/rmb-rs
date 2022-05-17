use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Mutex};

use super::{
    worker::{Worker, WorkerHandle},
    Work,
};

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
        let (sender, receiver) = mpsc::channel(size);

        for id in 0..size {
            Worker::new(sender.clone()).await.run().await;
        }

        WorkerPool { receiver }
    }

    pub async fn recv(&mut self) -> WorkerHandle<W> {
        let sender = self.receiver.recv().await.unwrap();
        WorkerHandle { sender }
    }
}
