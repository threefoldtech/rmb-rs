use crate::storage::Storage;
use anyhow::Result;
use std::time::Duration;

const MIN_RETRIES: usize = 1;
const MAX_RETRIES: usize = 5;

pub struct Peer<S: Storage> {
    id: u32,
    storage: S,
}

impl<S> Peer<S>
where
    S: Storage,
{
    pub fn new(id: u32, storage: S) -> Self {
        Self { id, storage }
    }

    pub async fn start() -> Result<()> {
        unimplemented!()
    }
}

fn between<T: Ord>(v: T, min: T, max: T) -> T {
    if v < min {
        return min;
    } else if v > max {
        return max;
    }

    v
}

/// processor processes the local client queues, and fill up the message for processing
/// before pushing it to the forward queue. where they gonna be picked up by the workers
pub async fn processor<S: Storage>(storage: S) {
    use tokio::time::sleep;
    let wait = Duration::from_secs(1);
    loop {
        let msg = match storage.local().await {
            Ok(msg) => msg,
            Err(err) => {
                log::error!("failed to process local messages: {}", err);
                sleep(wait).await;
                continue;
            }
        };

        let envelope = match msg.try_into() {
            Ok(envelop) => envelop,
            Err(err) => {
                log::error!("failed to build envelope message: {}", err);
                continue;
            }
        };

        // push message to forward.
        if let Err(err) = storage.forward(&envelope).await {
            log::error!("failed to push message for forwarding: {}", err);
        }
    }
}
