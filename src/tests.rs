use super::processor;
use super::storage::{ProxyStorage, RedisStorage, Storage};
use crate::anyhow::{Context, Result};
use crate::http_api::HttpApi;
use crate::http_workers::HttpWorker;
use crate::identity;
use crate::identity::{Identity, Signer};
use crate::proxy::ProxyWorker;
use crate::redis;
use crate::twin::{Twin, TwinDB};
use crate::types::Message;
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::{
        AsyncCommands, ErrorKind, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs,
        Value,
    },
    RedisConnectionManager,
};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

const PREFIX: &str = "test";

#[derive(Default, Clone)]
struct InMemoryDB {
    pub twins: HashMap<u32, Twin>,
}

impl InMemoryDB {
    fn add(&mut self, twin: Twin) {
        self.twins.insert(twin.id, twin);
    }
}

#[async_trait::async_trait]
impl TwinDB for InMemoryDB {
    async fn get_twin(&self, twin_id: u32) -> anyhow::Result<Option<Twin>> {
        Ok(self.twins.get(&twin_id).map(|t| t.clone()))
    }

    async fn get_twin_with_account(
        &self,
        _account_id: sp_runtime::AccountId32,
    ) -> anyhow::Result<u32> {
        unimplemented!()
    }
}

fn new_message(
    command: &str,
    expiration: u64,
    retry: usize,
    data: &str,
    destination: Vec<u32>,
) -> Message {
    //let id = format!("{}", uuid::Uuid::new_v4());
    let ret_queue = format!("{}", uuid::Uuid::new_v4());
    let epoch = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    Message {
        command: String::from(command),
        expiration: expiration,
        retry: retry,
        data: data.to_string(),
        destination: destination,
        reply: ret_queue,
        timestamp: epoch,
        ..Message::default()
    }
}

async fn send_all(messages: Vec<Message>, local_redis: &str) -> Result<(usize, Vec<String>)> {
    let pool = redis::pool(String::from(local_redis))
        .await
        .context("unable to create redis connection")?;
    let mut conn = pool.get().await.context("unable to get redis connection")?;
    let queue = "msgbus.system.local";
    let mut responses_expected = 0;
    let mut return_queues = Vec::new();
    for msg in messages {
        let _ = conn.lpush(&queue, msg.clone()).await?;
        responses_expected += msg.destination.len();
        return_queues.push(msg.reply);
    }
    Ok((responses_expected, return_queues))
}

async fn wait_for_responses(
    responses_expected: usize,
    return_queues: Vec<String>,
    timeout: usize,
    local_redis: &str,
) -> Result<(Vec<Message>, usize, usize)> {
    let pool = redis::pool(String::from(local_redis))
        .await
        .context("unable to create redis connection")?;
    let mut conn = pool.get().await.context("unable to get redis connection")?;
    let mut responses = Vec::new();
    let mut err_count = 0;
    let mut success_count = 0;
    for _ in 0..responses_expected {
        let result: Option<(String, String)> = conn
            .blpop(return_queues.clone(), timeout)
            .await
            .context("unable to get response")?;
        if result.is_none() {
            break;
        }
        let response = serde_json::from_str::<Message>(&result.unwrap().1)
            .context("unable to parse response")?;
        responses.push(response.clone());
        if response.error.is_some() {
            err_count += 1;
        } else {
            success_count += 1;
        }
    }
    Ok((responses, err_count, success_count))
}

async fn handle_cmd(cmd: &str, redis: &str) -> Result<()> {
    let pool = redis::pool(String::from(redis))
        .await
        .context("unable to create redis connection")?;
    let mut conn = pool.get().await.context("unable to get redis connection")?;
    loop {
        let result: Option<(String, String)> = conn
            .blpop(format!("msgbus.{}", cmd), 100)
            .await
            .context("unable to get response")?;
        if result.is_none() {
            ()
        }
        let mut response: Message = serde_json::from_str::<Message>(&result.unwrap().1)
            .context("unable to parse response")?;
        response.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        (response.destination, response.source) = (vec![response.source], response.destination[0]);
        let _ = conn
            .lpush(&response.reply, &response)
            .await
            .context("unable to push response")?;
        println!("replay sent");
    }
}

async fn start_rmb<
    D: TwinDB + Clone,
    S: Storage + ProxyStorage + Clone,
    I: Identity + Signer + 'static,
>(
    db: D,
    storage: S,
    ident: I,
    address: &str,
    id: &u32,
) -> Result<()> {
    let processor_handler = tokio::spawn(processor(*id, storage.clone()));

    let api_handler =
        tokio::spawn(HttpApi::new(*id, address, storage.clone(), ident.clone(), db.clone())?.run());

    let proxy_handler = tokio::spawn(ProxyWorker::new(*id, 10, storage.clone(), db.clone()).run());

    let workers_handler = tokio::task::spawn(HttpWorker::new(1000, storage, db, ident).run());

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
    };
    unreachable!();
}

#[tokio::test]
async fn test_end_to_end() {
    simple_logger::SimpleLogger::new()
        .with_level({ log::LevelFilter::Debug })
        .with_module_level("hyper", log::LevelFilter::Off)
        .with_module_level("ws", log::LevelFilter::Off)
        .with_module_level("substrate_api_client", log::LevelFilter::Off)
        .init()
        .unwrap();

    const WORDS1: &str = "neck stage box cup core magic produce exercise happy rely vocal then";
    const WORDS2: &str =
        "nominee slow sting tell point bleak sheriff outer push visual basket grief";
    // load from WORD
    let t1 = identity::Ed25519Signer::try_from(WORDS1).unwrap();
    let t2 = identity::Ed25519Signer::try_from(WORDS2).unwrap();

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
    // creating mock db
    let mut db1 = InMemoryDB::default();
    // insert dummy entities into mock db
    db1.add(twin1.clone());
    db1.add(twin2.clone());
    let db2 = db1.clone();

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
    tokio::spawn(async move { start_rmb(db1, storage1, t1, &twin1.address, &twin1.id).await });
    tokio::spawn(async move { start_rmb(db2, storage2, t2, &twin2.address, &twin2.id).await });
    // mimic a process that handle a command `testme` from a remote node
    let cmd = "testme";
    let remote_redis = "redis://127.0.0.1:6380";
    tokio::spawn(async move { handle_cmd(cmd, remote_redis).await.unwrap() });

    // test simple message exchange
    // this expects that two redis instances are running on localhost:6379 and localhost:6380
    // create a new message
    let msg = new_message(
        "testme" as &str,
        120,
        3,
        "TestDataTestDataTestDataTestData",
        vec![twin2.id, twin2.id],
    );

    // send 20 messages
    let messages = std::iter::repeat_with(|| msg.clone())
        .take(20)
        .collect::<Vec<_>>();

    let (responses_expected, return_queues) =
        send_all(messages, "redis://localhost:6379").await.unwrap();
    // wait for all messages to be processed
    let (responses, err_count, success_count) = wait_for_responses(
        responses_expected,
        return_queues,
        1000,
        "redis://localhost:6379",
    )
    .await
    .unwrap();
    assert_eq!(err_count, 0);
    assert_eq!(success_count, responses_expected);
    println!("{}", responses.len());
}
