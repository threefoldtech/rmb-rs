use tokio::sync::{mpsc, oneshot};

use super::{Job, Work};

pub struct Worker<W: Work> {
    work: W,
    sender: mpsc::Sender<Job<W>>,
}

impl<W> Worker<W>
where
    W: Work + Send + Sync + 'static,
{
    pub fn new(work: W, sender: mpsc::Sender<Job<W>>) -> Self {
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
                    Ok(job) => {
                        let result = self.work.run(job.0).await;
                        if let Some(ch) = job.1 {
                            let _ = ch.send(result);
                        }
                    }
                    Err(_) => {
                        log::debug!("worker handler dropped without receiving a job");
                    }
                };
            }
        });
    }
}
