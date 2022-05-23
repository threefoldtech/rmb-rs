#![allow(dead_code)]
#![allow(unused)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use anyhow::Context;
use cache::RedisCache;
use http_api::HttpApi;
use identity::Ed25519Identity;
use identity::Identity;
use storage::RedisStorage;
use twin::{SubstrateTwinDB, TwinDB};
mod cache;
mod http_api;
mod http_workers;
mod identity;
mod storage;
mod twin;
mod types;
mod workers;

#[tokio::main]
async fn main() {
    // let http_worker = http_workers::HttpWorker::new(10, RedisStorage).await;
    // http_worker.run().await;

    // tokio::time::sleep(std::time::Duration::from_secs(1000)).await;
    // return;

    let db = SubstrateTwinDB::<RedisCache>::new("wss://tfchain.dev.grid.tf", None)
        .context("cannot create substrate twin db object")
        .unwrap();

    let storage = RedisStorage;
    let identity = Ed25519Identity::try_from("<MNEMONICS>").unwrap();
    let account_id = identity.get_public_key();
    let twin_id = db
        .get_twin_id(account_id)
        .await
        .context("can not get twin id")
        .unwrap();

    HttpApi::new("127.0.0.1", 8888, storage, identity, twin_id)
        .unwrap()
        .run()
        .await
        .unwrap();
}
