use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::Work;

pub struct Worker<W> {
    sender: mpsc::Sender<oneshot::Sender<W>>,
}

impl<W> Worker<W>
where
    W: Work + Send + Sync + 'static,
{
    pub async fn new(sender: mpsc::Sender<oneshot::Sender<W>>) -> Self {
        Self { sender }
    }
    pub async fn run(self) {
        tokio::spawn(async move {
            loop {
                let (tx, rx) = oneshot::channel();

                if let Err(_err) = self.sender.send(tx).await {
                    break;
                }

                match rx.await {
                    Ok(job) => job.run().await,
                    Err(e) => {
                        log::debug!("worker handler dropped without receiving a job");
                    }
                };
            }
        });
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
