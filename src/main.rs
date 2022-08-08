use std::env;
use std::fmt::{Debug, Display};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueHint};

use rmb::cache::RedisCache;
use rmb::http_api::{HttpApi, UploadConfig};
use rmb::http_workers::HttpWorker;
use rmb::identity;
use rmb::identity::Identity;
use rmb::processor;
use rmb::proxy::ProxyWorker;
use rmb::redis;
use rmb::storage::RedisStorage;
use rmb::twin::{SubstrateTwinDB, TwinDB};

const GIT_VERSION: &str =
    git_version::git_version!(args = ["--tags", "--always", "--dirty=-modified"]);

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

/// the reliable message bus
#[derive(Parser, Debug)]
#[clap(name ="rmb", author, version = GIT_VERSION, about, long_about = None)]
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
    uploads: bool,

    /// where to save uploaded files (default is environment temp directory)
    #[clap(long, parse(from_os_str), value_hint = ValueHint::FilePath)]
    upload_dir: Option<PathBuf>,

    /// redis address
    #[clap(short, long, default_value_t = String::from("redis://localhost:6379"))]
    redis: String,

    /// substrate address please make sure the url also include the port number
    #[clap(short, long, default_value_t = String::from("wss://tfchain.grid.tf:443"))]
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
        .with_module_level("mpart_async", log::LevelFilter::Off)
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

    // uploads config
    let upload_config = if args.uploads {
        let dir = match &args.upload_dir {
            Some(path) => path.clone(),
            None => env::temp_dir(),
        };
        if !dir.is_dir() {
            bail!(
                "provided upload directory of '{:?}' does not exist or is not a directory",
                dir
            );
        }
        UploadConfig::Enabled(dir)
    } else {
        UploadConfig::Disabled
    };

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
            upload_config,
        )?
        .run(),
    );

    let proxy_handler =
        tokio::spawn(ProxyWorker::new(id, 10, storage.clone(), db.clone(), identity.clone()).run());

    // spawn the http worker
    let workers_handler =
        tokio::task::spawn(HttpWorker::new(args.workers, storage, db, identity).run());

    // handlers are Result<result, Error>
    tokio::select! {
        result = processor_handler => {
            if let Err(err) = result {
                bail!("message processor panicked unexpectedly: {}", err);
            }
        },
        result = api_handler => {
            match result {
                Err(err) => bail!("http server panicked unexpectedly: {}", err),
                Ok(Ok(_)) => bail!("http server exited unexpectedly"),
                Ok(Err(err)) => bail!("http server exited with error: {}", err),
            }
        },
        result = workers_handler => {
            if let Err(err) = result {
                bail!("http workers panicked unexpectedly: {}", err);
            }
        },
        result = proxy_handler => {
            if let Err(err) = result {
                bail!("proxy workers panicked unexpectedly: {}", err);
            }
        },
    }

    log::warn!("unreachable");
    unreachable!();
}

/// set_ca populate the SSL_CERT_DIR environment variable
/// only if built against musl and none of the SSL variables
/// are passed by the user.
fn set_ca() {
    if std::cfg!(target_env = "musl") {
        let file = env::var_os("SSL_CERT_FILE");
        let dir = env::var_os("SSL_CERT_DIR");
        if file.is_some() || dir.is_some() {
            // user already setting up environment file
            // for certificate
            return;
        }

        // nothing is set, override
        env::set_var("SSL_CERT_DIR", "/etc/ssl/certs")
    }
}

#[tokio::main]
async fn main() {
    // we set the soft, hard limit of max number of open file to a big value so we can handle as much connections
    // as possible.
    use nix::sys::resource::{getrlimit, setrlimit, Resource};
    let (_, max) = getrlimit(Resource::RLIMIT_NOFILE)
        .context("failed to get rlimit")
        .unwrap();

    const MAX_NOFILE: u64 = 63185;
    let max = if max < MAX_NOFILE {
        log::warn!(
            "maximum possible connections is set at '{}' please run as root for higher value",
            max
        );
        max
    } else {
        MAX_NOFILE
    };

    if let Err(err) = setrlimit(Resource::RLIMIT_NOFILE, max, max) {
        log::warn!("failed to increase max number of open files: {}", err)
    }

    set_ca();

    let args = Args::parse();
    if let Err(e) = app(&args).await {
        eprintln!("{:#}", e);
        std::process::exit(1);
    }
}
