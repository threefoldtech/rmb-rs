
use super::processor;
use crate::anyhow::{Context, Result};
use crate::http_api::HttpApi;
use crate::http_workers::HttpWorker;
use crate::identity;
use crate::identity::{Identity, Signer};
use crate::proxy::ProxyWorker;
use crate::redis;
use super::storage::{ProxyStorage, RedisStorage, Storage};
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::{
        AsyncCommands, ErrorKind, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs,
        Value,
    },
    RedisConnectionManager,
};
use crate::twin::{Twin, TwinDB};
use std::collections::HashMap;
use crate::types::Message;

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

async fn create_redis_storage() -> RedisStorage {
    let manager = RedisConnectionManager::new("redis://127.0.0.1/")
        .context("unable to create redis connection manager")
        .unwrap();
    let pool = Pool::builder()
        .build(manager)
        .await
        .context("unable to build pool or redis connection manager")
        .unwrap();
    let storage = RedisStorage::builder(pool)
        .prefix(PREFIX)
        .max_commands(500)
        .build();

    storage
}

// def new_message(command: str, twin_dst: list, data: dict = {}, expiration: int = 120, retry: int = 3):
//     version = 1
//     id =  str(uuid.uuid4()) # RMB-rs will override this id
//     twin_src = 0
//     retqueue = str(uuid.uuid4())
//     schema = ""
//     epoch = int(time.time())
//     err = ""
//     return Message(version, id, command, expiration, retry, data, twin_src, twin_dst, retqueue, schema, epoch, err)

fn new_message(command: &str, expiration: u64, retry: usize, data: &str, destination: Vec<u32>) -> Message {
    //let id = format!("{}", uuid::Uuid::new_v4());
    let ret_queue = format!("{}", uuid::Uuid::new_v4());
    let epoch = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    Message {
        command: command.to_string(),
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
    let queue = format!("{}:{}", PREFIX, "msgbus.system.local");
    let mut responses_expected = 0;
    let mut return_queues = Vec::new();
    for msg in messages {
        let _ = conn.lpush(&queue, msg.clone()).await?;
        responses_expected += msg.destination.len();
        return_queues.push(msg.reply);
    }
    Ok((responses_expected, return_queues))
}

// def wait_all(responses_expected, return_queues, timeout=20):
//         responses = []
//         err_count = 0
//         success_count = 0
//         
//             for i in range(responses_expected):
//                 result = r.blpop(return_queues, timeout=timeout)
//                 if not result:
//                     break
//                 response = json.loads(result[1])
//                 responses.append(response)
//                 if response["err"]:
//                     err_count += 1
//                     bar.text('received an error ❌')
//                 else:
//                     success_count += 1
//                     
//         return responses, err_count, success_count

async fn wait_all(
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

async fn start_rmb<
    D: TwinDB + Clone,
    S: Storage + ProxyStorage + Clone,
    I: Identity + Signer + 'static,
>(
    db: D,
    storage: S,
    ident: I,
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
    let mut db2 = db1.clone();

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
