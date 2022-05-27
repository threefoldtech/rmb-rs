#![allow(dead_code)]
#![allow(unused)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use std::time::Duration;

use anyhow::Context;
use bb8_redis::{bb8::Pool, RedisConnectionManager};

use http_api::HttpApi;
use identity::Ed25519Identity;
use storage::RedisStorage;
use storage::Storage;

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

    let manager = RedisConnectionManager::new("redis://127.0.0.1/")
        .context("unable to create redis connection manager")
        .unwrap();
    let pool = Pool::builder()
        .build(manager)
        .await
        .context("unable to build pool or redis connection manager")
        .unwrap();

    const PREFIX: &str = "msgbus";
    const TTL: Duration = Duration::from_secs(20);
    const MAX_COMMANDS: isize = 500;

    let storage = RedisStorage::builder(pool)
        .prefix(PREFIX)
        .ttl(TTL)
        .max_commands(500)
        .build();

    let ret = storage.local().await;
    match ret {
        Ok(msg) => {
            println!("{:?}", msg);
        }
        Err(e) => {
            println!("{:?}", e);
        }
    }

    // let identity = Ed25519Identity::try_from("value").unwrap();

    // HttpApi::new("127.0.0.1", 888, storage, identity)
    //     .unwrap()
    //     .run()
    //     .await
    //     .unwrap();
}
