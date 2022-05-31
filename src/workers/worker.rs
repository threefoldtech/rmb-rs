use tokio::sync::{mpsc, oneshot};

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

                if self.sender.send(tx).await.is_err() {
                    log::debug!("worker exiting");
                    break;
                }

                match rx.await {
                    Ok(job) => self.work.run(job).await,
                    Err(_) => {
                        log::debug!("worker handler dropped without receiving a job");
                    }
                };
            }
        });
    }
}
