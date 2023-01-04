use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use rmb::cache::RedisCache;
use rmb::identity::Identity;
use rmb::identity::KeyType;
use rmb::peer::storage::RedisStorage;
use rmb::relay;
use rmb::twin::{SubstrateTwinDB, TwinDB};
use rmb::{identity, redis};

/// A peer requires only which rely to connect to, and
/// which identity (mnemonics)

/// the reliable message bus
#[derive(Parser, Debug)]
#[clap(name ="rmb-rely", author, version = env!("GIT_VERSION"), about, long_about = None)]
struct Args {
    /// domain of this relay or it's public IP. used to identify
    /// if a twin is on this relay or not.
    #[clap(short = 'm', long)]
    domain: String,

    /// redis address
    #[clap(short, long, default_value_t = String::from("redis://localhost:6379"))]
    redis: String,

    /// substrate address please make sure the url also include the port number
    #[clap(short, long, default_value_t = String::from("wss://tfchain.grid.tf:443"))]
    substrate: String,

    /// enable debugging logs
    #[clap(short, long)]
    debug: bool,
}

async fn app(args: &Args) -> Result<()> {
    //ed25519 seed.
    //let seed = "0xb2643a23e021c2597ad2902ac8460057165af2b52b734300ae1214cffe384816";
    simple_logger::SimpleLogger::new()
        .with_level(if args.debug {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Info
        })
        .with_module_level("hyper", log::LevelFilter::Off)
        .with_module_level("ws", log::LevelFilter::Off)
        .with_module_level("substrate_api_client", log::LevelFilter::Off)
        .with_module_level("mpart_async", log::LevelFilter::Off)
        .with_module_level("jsonrpsee_core", log::LevelFilter::Off)
        .init()?;

    let pool = redis::pool(&args.redis)
        .await
        .context("failed to initialize redis pool")?;

    let db = SubstrateTwinDB::<RedisCache>::new(
        &args.substrate,
        RedisCache::new(pool.clone(), "twin", Duration::from_secs(600)),
    )
    .await
    .context("cannot create substrate twin db object")?;

    let opt = relay::SwitchOptions::new(pool);
    let r = relay::Relay::new(opt).await.unwrap();

    r.start("0.0.0.0:8080").await.unwrap();
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = app(&args).await {
        eprintln!("{:#}", e);
        std::process::exit(1);
    }
}
