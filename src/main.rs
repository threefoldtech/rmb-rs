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
mod identity;
mod storage;
mod twin;
mod types;

#[tokio::main]
async fn main() {
    let storage = RedisStorage;
    let identity = Ed25519Identity::try_from("value").unwrap();

    HttpApi::new("127.0.0.1", 888, storage, identity)
        .unwrap()
        .run()
        .await
        .unwrap();
}
