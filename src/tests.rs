use super::processor;
use crate::cache::Cache;
use crate::cache::MemCache;
use crate::http_workers::HttpWorker;
use crate::identity;
use crate::identity::Identity;
use crate::proxy::ProxyWorker;
use crate::redis;
use crate::storage::{RedisStorage, Storage};

use crate::twin::{SubstrateTwinDB, Twin};

use crate::anyhow::{Context, Result};
use crate::http_api::HttpApi;
use crate::twin::TwinDB;

async fn start_rmb(
    db: SubstrateTwinDB<MemCache<Twin>>,
    storage: RedisStorage,
    ident: identity::Ed25519Signer,
    address: String,
) -> Result<()> {
    let processor_handler_1 = tokio::spawn(processor(1, storage.clone()));

    let api_handler_1 =
        tokio::spawn(HttpApi::new(1, address, storage.clone(), ident.clone(), db.clone())?.run());

    let proxy_handler_1 = tokio::spawn(ProxyWorker::new(1, 10, storage.clone(), db.clone()).run());

    let workers_handler_1 = tokio::task::spawn(HttpWorker::new(1000, storage, db, ident).run());

    tokio::select! {
        result = processor_handler_1 => {
            if let Err(err) = result {
                bail!("message processor panicked unexpectedly: {}", err);
            }
        },
        result = api_handler_1 => {
            match result {
                Err(err) => bail!("http server panicked unexpectedly: {}", err),
                Ok(Ok(_)) => bail!("http server exited unexpectedly"),
                Ok(Err(err)) => bail!("http server exited with error: {}", err),
            }
        },
        result = workers_handler_1 => {
            if let Err(err) = result {
                bail!("http workers panicked unexpectedly: {}", err);
            }
        },
        result = proxy_handler_1 => {
            if let Err(err) = result {
                bail!("proxy workers panicked unexpectedly: {}", err);
            }
        },
    };
    unreachable!();
}

#[tokio::test]
async fn test_end_to_end() {
    const WORDS1: &str = "neck stage box cup core magic produce exercise happy rely vocal then";
    const WORDS2: &str =
        "nominee slow sting tell point bleak sheriff outer push visual basket grief";
    // load from WORD
    let t1 = identity::Ed25519Signer::try_from(WORDS1).unwrap();
    let t2 = identity::Ed25519Signer::try_from(WORDS2).unwrap();

    // create cache
    let mem: MemCache<Twin> = MemCache::new();
    // create dummy entities for testing
    let twin1: Twin = Twin {
        version: 1,
        id: 1,
        account: t1.account(),
        address: "127.0.0.1:5810".to_string(),
        entities: vec![],
    };
    let twin2: Twin = Twin {
        version: 1,
        id: 2,
        account: t2.account(),
        address: "127.0.0.1:5820".to_string(),
        entities: vec![],
    };
    // insert dummy entities into cache
    mem.set(1, twin1.clone())
        .await
        .context("can not set value to cache")
        .unwrap();
    mem.set(2, twin2.clone())
        .await
        .context("can not set value to cache")
        .unwrap();

    // create db
    let db1 =
        SubstrateTwinDB::<MemCache<Twin>>::new("wss://tfchain.dev.grid.tf", mem.clone()).unwrap();
    let db2 =
        SubstrateTwinDB::<MemCache<Twin>>::new("wss://tfchain.dev.grid.tf", mem.clone()).unwrap();

    // test get fake twin id
    let twin = db1
        .get_twin(1)
        .await
        .context("failed to get own twin id")
        .unwrap();
    assert_eq!(twin, Some(twin1.clone()));

    // create redis storage
    let pool1 = redis::pool("redis://localhost:6379")
        .await
        .context("failed to initialize redis pool")
        .unwrap();
    let pool2 = redis::pool("redis://localhost:6380")
        .await
        .context("failed to initialize redis pool")
        .unwrap();

    let storage1 = RedisStorage::builder(pool1).build();
    let storage2 = RedisStorage::builder(pool2).build();

    // start two instances of RMB
    tokio::spawn(async { start_rmb(db1, storage1, t1, twin1.address).await });
    tokio::spawn(async { start_rmb(db2, storage2, t2, twin2.address).await });

    // TODO: simple flow to test rmb
    // this expects that two redis instances are running on localhost:6379 and localhost:6380
    // push a message to storage1 on local queue
    // should be received by storage2 on $cmd queue
    // mimic a process that handle that command and push a reply to storage2 on reply queue
    // should be received by storage1 on ret queue
}
