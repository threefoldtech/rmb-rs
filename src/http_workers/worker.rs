use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task;

use super::Msg;

pub struct Worker {
    id: usize,
    task: Option<task::JoinHandle<()>>,
}

impl Worker {
    pub async fn new(id: usize, mut receiver: Arc<Mutex<mpsc::Receiver<Msg>>>) -> Worker {
        let task = tokio::spawn(async move {
            loop {
                let message = receiver.lock().await.recv().await;
                match message {
                    Some(Msg::NewJob(job)) => {
                        job();
                    }
                    Some(Msg::Terminate) => {
                        break;
                    }
                    None => {
                        todo!()
                    }
                }
            }
        });

        Worker {
            id,
            task: Some(task),
        }
    }
}
