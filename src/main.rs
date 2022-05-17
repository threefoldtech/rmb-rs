#![allow(dead_code)]
#![allow(unused)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use http_api::HttpApi;
use identity::Ed25519Identity;
use storage::RedisStorage;
mod cache;
mod http_api;
mod http_workers;
mod identity;
mod storage;
mod twin;
mod types;

#[tokio::main]
async fn main() {
    // let http_worker = http_workers::HttpWorker::new(10, RedisStorage).await;
    // http_worker.run().await;

    // tokio::time::sleep(std::time::Duration::from_secs(1000)).await;
    // return;

    let storage = RedisStorage;
    let identity = Ed25519Identity::try_from("value").unwrap();

    HttpApi::new("127.0.0.1", 888, storage, identity)
        .unwrap()
        .run()
        .await
        .unwrap();
}
