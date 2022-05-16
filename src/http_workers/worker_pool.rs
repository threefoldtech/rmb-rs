use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};

use super::{worker::Worker, Msg};

pub struct WorkerPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Msg>,
}

impl WorkerPool {
    pub async fn new(size: usize) -> WorkerPool {
        let (sender, receiver) = mpsc::channel(size);

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)).await);
        }

        WorkerPool { workers, sender }
    }

    pub async fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender.send(Msg::NewJob(job)).await;
    }
}
