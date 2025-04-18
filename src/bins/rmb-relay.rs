use std::num::NonZeroUsize;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{builder::ArgAction, Parser};
use rmb::cache::RedisCache;
use rmb::events;
use rmb::redis;
use rmb::relay::{
    self,
    limiter::{FixedWindowOptions, Limiters},
};
use rmb::twin::SubstrateTwinDB;
use tokio::sync::oneshot;

/// A peer requires only which rely to connect to, and
/// which identity (mnemonics)
/// the reliable message bus
#[derive(Parser, Debug)]
#[clap(name ="rmb-rely", author, version = env!("GIT_VERSION"), about, long_about = None)]
struct Args {
    /// domains of this relay or it's public IPs. used to identify
    /// if a twin is on this relay or not.
    #[clap(short = 'm', long = "domain", num_args = 1..)]
    domains: Vec<String>,

    /// redis address
    #[clap(short, long, default_value_t = String::from("redis://localhost:6379"))]
    redis: String,

    /// substrate addresses please make sure the url also include the port number
    #[clap(
        short,
        long,
        default_value = "wss://tfchain.grid.tf:443",
        num_args = 1..,
    )]
    substrate: Vec<String>,

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

    /// limits used by the rate limiter. basically a user will be only permited to send <count> messages with size <size> in a time window (usually a minute).
    #[clap(long, num_args=2, value_names=["count", "size"])]
    limit: Vec<usize>,

    /// period in seconds used by ranker to determine the recent period of time during which failures will be considered.
    /// failures that occurred outside this specified period will be disregarded.
    #[clap(short = 'p', long, default_value_t = 3600)]
    ranker_period: u64,
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

async fn app(args: Args, tx: oneshot::Sender<()>) -> Result<()> {
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

    let redis_cache = RedisCache::new(pool.clone(), "twin");

    let twins = SubstrateTwinDB::<RedisCache>::new(args.substrate.clone(), redis_cache.clone())
        .await
        .context("cannot create substrate twin db object")?;

    let max_users = args.workers as usize * args.user_per_worker as usize;
    let opt = relay::SwitchOptions::new(pool.clone())
        .with_workers(args.workers)
        .with_max_users(max_users);

    let federation = relay::FederationOptions::new(pool).with_workers(fed_size as usize);

    let cache_capacity = NonZeroUsize::new(max_users * 2).unwrap();
    let limiter = if args.limit.is_empty() {
        Limiters::no_limit()
    } else {
        Limiters::fixed_window(
            cache_capacity,
            FixedWindowOptions {
                count: args.limit[0],
                size: args.limit[1],
                window: 60,
            },
        )
    };
    let ranker = relay::ranker::RelayRanker::new(Duration::from_secs(args.ranker_period));
    let r = relay::Relay::new(
        args.domains.iter().cloned().collect(),
        twins,
        opt,
        federation,
        limiter,
        ranker,
    )
    .context("failed to initialize relay")?;

    let mut l = events::Listener::new(args.substrate, redis_cache).await?;
    tokio::spawn(async move {
        let max_retries = 9; // max wait is 2^9 = 512 seconds ( 5 minutes )
        let mut attempt = 0;
        let mut backoff = Duration::from_secs(1);
        let mut got_hit = false;

        loop {
            match l
                .listen(&mut got_hit)
                .await
                .context("failed to listen to chain events")
            {
                Ok(_) => break,
                Err(e) => {
                    if got_hit {
                        log::warn!("Listener got a hit, but failed to listen to chain events before no attempts will be reset");
                        got_hit = false;
                        attempt = 0;
                        backoff = Duration::from_secs(1);
                    }
                    attempt += 1;
                    if attempt > max_retries {
                        log::error!("Listener failed after {} attempts: {:?}", attempt - 1, e);
                        let _ = tx.send(());
                        break;
                    }
                    log::warn!(
                        "Listener failed on attempt {}: {:?}. Retrying in {:?}...",
                        attempt,
                        e,
                        backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
    });

    r.start(&args.listen).await.unwrap();
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let (tx, rx) = oneshot::channel();
    let app_handle = tokio::spawn(async move {
        if let Err(e) = app(args, tx).await {
            eprintln!("{:#}", e);
            std::process::exit(1);
        }
    });

    tokio::select! {
        _ = app_handle => {
            log::info!("Application is closing successfully.");
        }
        _ = rx => {
            log::error!("Listener shutdown signal received. Exiting application.");
            std::process::exit(1);
        }
    }
}
