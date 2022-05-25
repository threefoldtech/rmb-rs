use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::Work;

pub struct Worker<W: Work> {
    work: W,
    sender: mpsc::Sender<oneshot::Sender<W::Job>>,
}

impl<W> Worker<W>
where
    W: Work + Send + Sync + 'static,
{
    pub fn new(work: W, sender: mpsc::Sender<oneshot::Sender<W::Job>>) -> Self {
        Self { work, sender }
    }
    pub fn run(self) {
        tokio::spawn(async move {
            loop {
                let (tx, rx) = oneshot::channel();

                if let Err(_err) = self.sender.send(tx).await {
                    break;
                }

                match rx.await {
                    Ok(job) => self.work.run(job).await,
                    Err(e) => {
                        log::debug!("worker handler dropped without receiving a job");
                    }
                };
            }
        });
    }
}
