use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{builder::ArgAction, Parser};
use rmb::cache::RedisCache;
use rmb::identity::KeyType;
use rmb::identity::{Identity, Signer};
use rmb::peer::Pair;
use rmb::peer::{self, storage::RedisStorage};
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
    #[clap(long, default_value_t = String::from("wss://relay.grid.tf:443"))]
    relay: String,

    /// enable debugging logs
    #[clap(short, long, action=ArgAction::Count)]
    debug: u8,

    /// skip twin update on chain if relay is not matching. only used for debugging
    #[clap(long = "no-update")]
    no_update: bool,
}

async fn app(args: &Args) -> Result<()> {
    //ed25519 seed.
    //let seed = "0xb2643a23e021c2597ad2902ac8460057165af2b52b734300ae1214cffe384816";
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

    let secret = match args.mnemonics {
        Some(ref m) => m,
        None => match args.seed {
            Some(ref s) => s,
            None => {
                bail!("either mnemonics or seed must be provided");
            }
        },
    };

    let sk = Pair::from_str(secret).context("failed to initialize encryption key")?;

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

    let pool = redis::pool(&args.redis, 20)
        .await
        .context("failed to initialize redis pool")?;

    // cache is a little bit tricky because while it improves performance it
    // makes changes to twin data takes at least 5 min before they are detected
    let db = SubstrateTwinDB::<RedisCache>::new(
        &args.substrate,
        RedisCache::new(pool.clone(), "twin", Duration::from_secs(60)),
    )
    .await
    .context("cannot create substrate twin db object")?;

    let id = db
        .get_twin_with_account(identity.account())
        .await
        .context("failed to get own twin id")?
        .ok_or_else(|| anyhow::anyhow!("no twin found on this network with given key"))?;

    let relay_url: url::Url = args.relay.parse().context("failed to parse relay url")?;

    if !args.no_update {
        // try to check and update the twin info on chain

        // we need to make sure our twin is up to date
        let twin = db
            .get_twin(id)
            .await
            .context("failed to get twin details")?
            .ok_or_else(|| anyhow::anyhow!("self twin not found!"))?;

        log::debug!("twin relay domain: {:?}", twin.relay);
        // if twin relay or his pk don't match the ones that
        // should be there, we need to set the value on chain
        if twin.relay.as_deref() != relay_url.domain()
            || !matches!(twin.pk, Some(ref pk) if pk == &sk.public())
        {
            // remote relay is not the same as configure one. update is needed
            log::info!("update twin details on the chain");

            let pk = sk.public();
            db.update_twin(
                &identity.pair(),
                relay_url.domain().map(|d| d.to_owned()),
                Some(&pk),
            )
            .await
            .context("failed to update twin information")?;
        }
    }

    let storage = RedisStorage::builder(pool).build();
    log::info!("twin: {}", id);

    let u = url::Url::parse(&args.relay)?;
    peer::start(u, id, identity, storage, db).await
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = app(&args).await {
        eprintln!("{:#}", e);
        std::process::exit(1);
    }
}
