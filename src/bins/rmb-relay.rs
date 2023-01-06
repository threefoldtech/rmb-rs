use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use rmb::cache::RedisCache;
use rmb::redis;
use rmb::relay;
use rmb::twin::SubstrateTwinDB;

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

    /// number of switch users. Each worker maintains a single connection to
    /// redis used for waiting on user messages. hence this need to be sain value
    /// defaults to 500
    #[clap(short, long, default_value_t = 500)]
    workers: u32,

    /// maximum number of users per worker. the total number of users supported
    /// by this process is then workers * user_per_worker before the switch start
    /// rejecting new connections.
    #[clap(short, long, default_value_t = 1000)]
    user_per_worker: u32,

    /// listen address
    #[clap(short, long, default_value_t = String::from("[::]:8080"))]
    listen: String,
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

    // we know that a worker requires one connection so pool must be min of number of workers
    // to have good preformance. but we also need a connection when a user sends a message to
    // push to the queue that depends on how fast messages are sent but we can assume an extra 10%
    // of number of workers is needed
    let pool_size = args.workers + std::cmp::max((args.workers * 10) / 100, 1);
    log::debug!("redis pool size: {}", pool_size);
    let pool = redis::pool(&args.redis, pool_size)
        .await
        .context("failed to initialize redis pool")?;

    let twins = SubstrateTwinDB::<RedisCache>::new(
        &args.substrate,
        RedisCache::new(pool.clone(), "twin", Duration::from_secs(600)),
    )
    .await
    .context("cannot create substrate twin db object")?;

    let opt = relay::SwitchOptions::new(pool)
        .with_workers(args.workers)
        .with_max_users(args.workers as usize * args.user_per_worker as usize);
    let r = relay::Relay::new(twins, opt).await.unwrap();

    r.start(&args.listen).await.unwrap();
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
