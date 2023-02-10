use std::time::Duration;

use anyhow::{Context, Result};
use clap::{builder::ArgAction, Parser};
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
    #[clap(short, long, action=ArgAction::Count)]
    debug: u8,
}

fn set_limits() -> Result<()> {
    // we set the soft, hard limit of max number of open file to a big value so we can handle as much connections
    // as possible.
    use nix::sys::resource::{getrlimit, setrlimit, Resource};
    let (_, max) = getrlimit(Resource::RLIMIT_NOFILE).context("failed to get rlimit")?;

    const MAX_NOFILE: u64 = 524288;
    let max = if max < MAX_NOFILE {
        log::warn!(
            "maximum possible connections is set at '{}' please run as root for higher value",
            max
        );
        MAX_NOFILE
    } else {
        max
    };

    log::debug!("setting rlimit(nofile) to: {}", max);
    if let Err(err) = setrlimit(Resource::RLIMIT_NOFILE, max, max) {
        log::error!("failed to increase max number of open files: {}", err);
    }

    Ok(())
}

async fn app(args: &Args) -> Result<()> {
    if args.workers == 0 {
        anyhow::bail!("number of workers cannot be zero");
    }

    if args.user_per_worker == 0 {
        anyhow::bail!("max number of users can't be zero");
    }

    simple_logger::SimpleLogger::new()
        .with_level({
            match args.debug {
                0 => log::LevelFilter::Info,
                1 => log::LevelFilter::Debug,
                _ => log::LevelFilter::Trace,
            }
        })
        .with_module_level("hyper", log::LevelFilter::Off)
        .with_module_level("ws", log::LevelFilter::Off)
        .with_module_level("substrate_api_client", log::LevelFilter::Off)
        .with_module_level("mpart_async", log::LevelFilter::Off)
        .with_module_level("jsonrpsee_core", log::LevelFilter::Off)
        .init()?;

    set_limits()?;
    // we know that a worker requires one connection so pool must be min of number of workers
    // to have good preformance. but we also need a connection when a user sends a message to
    // push to the queue that depends on how fast messages are sent but we can assume an extra 10%
    // of number of workers is needed

    // a wiggle is 10% of number of workers with min of 1
    let wiggle = std::cmp::max((args.workers * 10) / 100, 1);
    let pool_size = args.workers + wiggle;
    let fed_size = wiggle * 2;

    log::info!("redis pool size: {}", pool_size);
    log::info!("switch workers: {}", args.workers);
    log::info!("federation workers: {}", fed_size);
    log::info!(
        "max number of users: {}",
        args.workers * args.user_per_worker
    );

    let pool = redis::pool(&args.redis, pool_size)
        .await
        .context("failed to initialize redis pool")?;

    // we use 6 hours cache for twin information because twin id will not change anyway
    // and we only need twin public key for validation only.
    let twins = SubstrateTwinDB::<RedisCache>::new(
        &args.substrate,
        RedisCache::new(pool.clone(), "twin", Duration::from_secs(60)),
    )
    .await
    .context("cannot create substrate twin db object")?;

    let opt = relay::SwitchOptions::new(pool.clone())
        .with_workers(args.workers)
        .with_max_users(args.workers as usize * args.user_per_worker as usize);

    let federation = relay::Federation::new(pool).with_workers(fed_size as usize);
    let r = relay::Relay::new(&args.domain, twins, opt, federation)
        .await
        .unwrap();
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
