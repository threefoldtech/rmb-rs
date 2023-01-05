use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use rmb::cache::RedisCache;
use rmb::identity::Identity;
use rmb::identity::KeyType;
use rmb::peer::storage::RedisStorage;
use rmb::peer::Peer;
use rmb::twin::{SubstrateTwinDB, TwinDB};
use rmb::{identity, redis};

/// A peer requires only which rely to connect to, and
/// which identity (mnemonics)

/// the reliable message bus
#[derive(Parser, Debug)]
#[clap(name ="rmb", author, version = env!("GIT_VERSION"), about, long_about = None)]
struct Args {
    /// key type
    #[clap(short, long, default_value_t = KeyType::Sr25519)]
    key_type: KeyType,

    /// key mnemonics
    #[clap(short, long)]
    mnemonics: Option<String>,

    /// seed as hex (must start with 0x)
    #[clap(long, conflicts_with = "mnemonics")]
    seed: Option<String>,

    /// wither to accept uploads or not
    #[clap(short, long)]
    uploads: Option<PathBuf>,

    /// redis address
    #[clap(short, long, default_value_t = String::from("redis://localhost:6379"))]
    redis: String,

    /// substrate address please make sure the url also include the port number
    #[clap(short, long, default_value_t = String::from("wss://tfchain.grid.tf:443"))]
    substrate: String,

    /// substrate address please make sure the url also include the port number
    #[clap(long, default_value_t = String::from("wss://rely.grid.tf:443"))]
    relay: String,

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

    let secret = match args.mnemonics {
        Some(ref m) => m,
        None => match args.seed {
            Some(ref s) => s,
            None => {
                bail!("either mnemonics or seed must be provided");
            }
        },
    };

    // // uploads config
    // let _upload_config = match args.uploads {
    //     None => UploadConfig::Disabled,
    //     Some(ref dir) => {
    //         if !dir.is_dir() {
    //             bail!(
    //                 "provided upload directory of '{:?}' does not exist or is not a directory",
    //                 dir
    //             );
    //         }
    //         UploadConfig::Enabled(dir.clone())
    //     }
    // };

    let identity = match args.key_type {
        KeyType::Ed25519 => {
            let sk = identity::Ed25519Signer::try_from(secret.as_str())
                .context("failed to load ed25519 key from mnemonics or seed")?;
            identity::Signers::Ed25519(sk)
        }
        KeyType::Sr25519 => {
            let sk = identity::Sr25519Signer::try_from(secret.as_str())
                .context("failed to load sr25519 key from mnemonics or seed")?;
            identity::Signers::Sr25519(sk)
        }
    };

    let pool = redis::pool(&args.redis)
        .await
        .context("failed to initialize redis pool")?;

    let db = SubstrateTwinDB::<RedisCache>::new(
        &args.substrate,
        RedisCache::new(pool.clone(), "twin", Duration::from_secs(600)),
    )
    .await
    .context("cannot create substrate twin db object")?;

    let id = match db
        .get_twin_with_account(identity.account())
        .await
        .context("failed to get own twin id")?
    {
        Some(id) => id,
        None => bail!("no twin found on this network with given key"),
    };

    let storage = RedisStorage::builder(pool).build();
    log::info!("twin: {}", id);

    let u = url::Url::parse(&args.relay)?;
    let peer = Peer::new(u, id, identity, storage, db).await;
    peer.start().await?;

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
