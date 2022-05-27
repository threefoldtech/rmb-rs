#![allow(dead_code)]
#![allow(unused)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use std::time::Duration;

use anyhow::Context;
use bb8_redis::{bb8::Pool, RedisConnectionManager};

use cache::RedisCache;
use http_api::HttpApi;
use identity::Ed25519Identity;
use identity::Identity;
use storage::RedisStorage;
use storage::Storage;

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

    let db = SubstrateTwinDB::<RedisCache>::new("wss://tfchain.dev.grid.tf", None)
        .context("cannot create substrate twin db object")
        .unwrap();

    let identity = Ed25519Identity::try_from(
        "junior sock chunk accident pilot under ask green endless remove coast wood",
    );

    HttpApi::new("127.0.0.1", 8888, storage, identity)
        .unwrap()
        .run()
        .await
        .unwrap();
}
