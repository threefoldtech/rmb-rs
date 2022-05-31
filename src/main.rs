#[macro_use]
extern crate anyhow;

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
use identity::Identity;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use storage::{RedisStorage, Storage};
use twin::{SubstrateTwinDB, TwinDB};

use crate::http_workers::HttpWorker;

const MIN_RETRIES: usize = 1;
const MAX_RETRIES: usize = 5;
const MIN_DURATION: usize = 10;
const MAX_DURATION: usize = 60 * 60;

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

fn between<T: Ord>(v: T, min: T, max: T) -> T {
    if v < min {
        return min;
    } else if v > max {
        return max;
    }

    return v;
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

    /// http api listen address
    #[clap(short, long, default_value_t = String::from("[::]:8051"))]
    listen: String,

    /// number of workers to send messages to remote
    /// rmbs
    #[clap(short, long, default_value_t = 1000)]
    workers: usize,

    /// enable debugging logs
    #[clap(short, long)]
    debug: bool,
}

async fn app(args: &Args) -> Result<()> {
    // we have to initialize a signer key based on the given
    // key type
    simple_logger::SimpleLogger::new()
        .with_level(if args.debug {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Info
        })
        .with_module_level("hyper", log::LevelFilter::Off)
        .with_module_level("ws", log::LevelFilter::Off)
        .with_module_level("substrate_api_client", log::LevelFilter::Off)
        .init()?;

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
    let processor_handler = tokio::spawn(processor(id, storage.clone()));

    // spawn the http api server
    let api_handler = tokio::spawn(
        HttpApi::new(
            id,
            &args.listen,
            storage.clone(),
            identity.clone(),
            db.clone(),
        )?
        .run(),
    );

    // spawn the http worker
    let workers_handler =
        tokio::task::spawn(HttpWorker::new(args.workers, storage, db, identity).run());

    // handlers are Result<result, Error>
    tokio::select! {
        result = processor_handler => {
            if let Err(err) = result {
                bail!("message processor panicked unexpectedly: {}", err);
            }
        }
        result = api_handler => {
            match result {
                Err(err) => bail!("http server panicked unexpectedly: {}", err),
                Ok(Ok(_)) => bail!("http server exited unexpectedly"),
                Ok(Err(err)) => bail!("http server exited with error: {}", err),
            }
        }
        result = workers_handler => {
            if let Err(err) = result {
                bail!("workers panicked unexpectedly: {}", err);
            }
        }
    }

    log::warn!("unreachable");
    unreachable!();
}

/// processor processes the local client queues, and fill up the message for processing
/// before pushing it to the forward queue. where they gonna be picked up by the workers
async fn processor<S: Storage>(id: u32, storage: S) {
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
        msg.expiration = between(msg.expiration, MIN_DURATION, MAX_DURATION);

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
