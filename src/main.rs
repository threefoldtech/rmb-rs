#![allow(dead_code)]
#![allow(unused)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate clap;

mod cache;
mod http_api;
mod http_workers;
mod identity;
mod redis;
mod storage;
mod twin;
mod types;
mod workers;

use anyhow::{Context, Result};
use cache::RedisCache;
use clap::Parser;
use http_api::HttpApi;
use identity::Ed25519Signer;
use identity::Identity;
use log::kv::Key;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use storage::{RedisStorage, Storage};
use twin::{SubstrateTwinDB, TwinDB};

#[derive(Debug)]
enum KeyType {
    Ed25519,
    Sr25519,
}

impl FromStr for KeyType {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ed25519" => Ok(KeyType::Ed25519),
            "sr25519" => Ok(KeyType::Sr25519),
            _ => Err("invalid key type"),
        }
    }
}

impl Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            KeyType::Ed25519 => "ed25519",
            KeyType::Sr25519 => "sr25519",
        };

        f.write_str(s)
    }
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// key type
    #[clap(short, long, default_value_t = KeyType::Ed25519)]
    key_type: KeyType,

    /// key mnemonics
    #[clap(short, long)]
    mnemonics: String,

    /// redis address
    #[clap(short, long, default_value_t = String::from("redis://localhost:6379"))]
    redis: String,

    /// substrate address
    #[clap(short, long, default_value_t = String::from("wss://tfchain.grid.tf"))]
    substrate: String,

    /// enable debugging logs
    #[clap(short, long)]
    debug: bool,
}

async fn app(args: &Args) -> Result<()> {
    // we have to initialize a signer key based on the given
    // key type
    let logger = simple_logger::SimpleLogger::new()
        .with_level(if args.debug {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Info
        })
        .with_module_level("ws", log::LevelFilter::Off)
        .with_module_level("substrate_api_client", log::LevelFilter::Off)
        .init();

    let identity = match args.key_type {
        KeyType::Ed25519 => {
            let sk = identity::Ed25519Signer::try_from(args.mnemonics.as_str())
                .context("failed to load ed25519 key from mnemonics")?;
            identity::Signers::Ed25519(sk)
        }
        KeyType::Sr25519 => {
            let sk = identity::Sr25519Signer::try_from(args.mnemonics.as_str())
                .context("failed to load sr25519 key from mnemonics")?;
            identity::Signers::Sr25519(sk)
        }
    };

    let pool = redis::pool(&args.redis)
        .await
        .context("failed to initialize redis pool")?;

    let db = SubstrateTwinDB::<RedisCache>::new(
        &args.substrate,
        Some(cache::RedisCache::new(
            pool.clone(),
            "twin",
            Duration::from_secs(600),
        )),
    )
    .context("cannot create substrate twin db object")?;

    let id = db
        .get_twin_with_account(identity.account())
        .await
        .context("failed to get own twin id")?;

    let storage = RedisStorage::builder(pool).build();
    log::info!("twin: {}", id);

    // spawn the processor
    let handler = tokio::spawn(processor(id, storage.clone()));

    // todo!:
    // - you need to spawn the http api server and the http workers here
    // - you need to use tokio::spawn so each of those services are running in their own task
    // - you collect all handlers here (like above)
    // - you need to do a select! on all handlers. so in case any of them exits, you need to log the error
    // and exit because the system can't work with any of those components down.

    // let storage = RedisStorage;
    // let identity = Ed25519Signer::try_from(
    //     "junior sock chunk accident pilot under ask green endless remove coast wood",
    // )
    // .unwrap();

    // HttpApi::new(1, "127.0.0.1:8888", storage, identity, db)
    //     .unwrap()
    //     .run()
    //     .await
    //     .unwrap();

    Ok(())
}

/// processor processes the local client queues, and fill up the message for processing
/// before pushing it to the forward queue. where they gonna be picked up by the workers
async fn processor<S: Storage>(id: u32, storage: S) {
    use std::time::Duration;
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
        // todo: validates values on expiration, retry, etc.. to make sure
        // the values are sane.
        // push message to forward.
        while let Err(err) = storage.forward(&msg).await {
            log::error!("failed to push message for forwarding: {}", err);
            sleep(wait).await;
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = app(&args).await {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
