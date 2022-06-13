use super::http_api::HttpApi;
use super::http_workers::HttpWorker;
use super::identity;
use super::identity::{Identity, Signer};
use super::processor;
use super::proxy::ProxyWorker;
use super::redis;
use super::storage::{ProxyStorage, RedisStorage, Storage};
use super::twin::{Twin, TwinDB};
use super::types::Message;
use anyhow::{Context, Result};
use bb8_redis::redis::AsyncCommands;
use std::collections::HashMap;

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
            .blpop(format!("msgbus.{}", cmd), 0)
            .await
            .context("unable to get response")?;
        let mut response: Message = serde_json::from_str::<Message>(&result.unwrap().1)
            .context("unable to parse response")?;

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
        .with_level(log::LevelFilter::Debug)
        .with_module_level("hyper", log::LevelFilter::Off)
        .with_module_level("ws", log::LevelFilter::Off)
        .with_module_level("substrate_api_client", log::LevelFilter::Off)
        .init()
        .unwrap();

    const WORDS1: &str = "neck stage box cup core magic produce exercise happy rely vocal then";
    const WORDS2: &str =
        "nominee slow sting tell point bleak sheriff outer push visual basket grief";
    // load from WORD
    let t1 = identity::Sr25519Signer::try_from(WORDS1).unwrap();
    let t2 = identity::Ed25519Signer::try_from(WORDS2).unwrap();

    // we expects to have two instances of redis-server running on localhost:6379 and localhost:6380 on the test machine
    let local_redis = "redis://127.0.0.1:6379";
    let remote_redis = "redis://127.0.0.1:6380";

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
    let pool1 = redis::pool(local_redis)
        .await
        .context("failed to initialize redis pool")
        .unwrap();
    let pool2 = redis::pool(remote_redis)
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
    tokio::spawn(async move { handle_cmd(cmd, remote_redis).await.unwrap() });
    // test simple message exchange
    test_messages_exchange(cmd, 1, local_redis).await;
    test_messages_exchange(cmd, 200, local_redis).await;
    test_messages_exchange(cmd, 1000, local_redis).await;
}

async fn test_messages_exchange(cmd: &str, msg_count: usize, local_redis: &str) {
    // create a new message
    let msg = new_message(cmd, 120, 3, "TestDataTestDataTestDataTestData", vec![2]);
    // duplicate the message
    let messages = std::iter::repeat_with(|| msg.clone())
        .take(msg_count)
        .collect::<Vec<_>>();
    // send the messages to local redis
    let (responses_expected, return_queues) = send_all(messages, local_redis).await.unwrap();
    // wait on the return queues
    let (_responses, err_count, success_count) = wait_for_responses(
        responses_expected,
        return_queues,
        60, // give up if you didn't get all responses and $timeout seconds have passed since the last response was received
        local_redis,
    )
    .await
    .unwrap();
    assert_eq!(err_count, 0);
    assert_eq!(success_count, responses_expected);
}
