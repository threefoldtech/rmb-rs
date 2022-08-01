#[macro_use]
extern crate anyhow;
extern crate mime;

pub mod cache;
pub mod http_api;
pub mod http_workers;
pub mod identity;
pub mod proxy;
pub mod redis;
pub mod storage;
pub mod twin;
pub mod types;

use std::time::Duration;
use storage::Storage;

const MIN_RETRIES: usize = 1;
const MAX_RETRIES: usize = 5;

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
pub async fn processor<S: Storage>(id: u32, storage: S) {
    use tokio::time::sleep;
    let wait = Duration::from_secs(1);
    loop {
        let mut msg = match storage.local().await {
            Ok(msg) => msg,
            Err(err) => {
                log::error!("failed to process local messages: {}", err);
                sleep(wait).await;
                continue;
            }
        };

        msg.version = 1;
        // set the source
        msg.source = id;
        // set the message id.
        msg.id = uuid::Uuid::new_v4().to_string();
        msg.retry = between(msg.retry, MIN_RETRIES, MAX_RETRIES);
        msg.stamp();

        // push message to forward.
        if let Err(err) = storage.forward(&msg).await {
            log::error!("failed to push message for forwarding: {}", err);
        }
    }
}
