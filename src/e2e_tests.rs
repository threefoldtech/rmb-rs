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
use sp_core::crypto::Pair;
use sp_core::{ed25519::Pair as EdPair, sr25519::Pair as SrPair};
use std::collections::HashMap;
use std::panic;
use std::process::Command;
use std::sync::Once;

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
    let pool = redis::pool(local_redis)
        .await
        .context("unable to create redis connection")?;
    let mut conn = pool.get().await.context("unable to get redis connection")?;
    let queue = "msgbus.system.local";
    let mut responses_expected = 0;
    let mut return_queues = Vec::new();
    for msg in messages {
        let _ = conn.lpush(&queue, &msg).await?;
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
) -> Result<(Vec<Message>, usize, usize, usize)> {
    let pool = redis::pool(local_redis)
        .await
        .context("unable to create redis connection")?;
    let mut conn = pool.get().await.context("unable to get redis connection")?;
    let mut responses = Vec::new();
    let mut err_count = 0;
    let mut success_count = 0;
    for _ in 0..responses_expected {
        let result: Option<(String, String)> = conn
            .blpop(&return_queues, timeout)
            .await
            .context("unable to get response")?;
        if result.is_none() {
            break;
        }
        let response = serde_json::from_str::<Message>(&result.unwrap().1)
            .context("unable to parse response")?;
        if response.error.is_some() {
            err_count += 1;
        } else {
            success_count += 1;
        }
        responses.push(response);
    }
    Ok((
        responses,
        err_count,
        success_count,
        responses_expected - (err_count + success_count),
    ))
}

async fn send_and_wait(
    cmd: &str,
    twin_dst: Vec<u32>,
    msg_count: usize,
    redis_port: usize,
) -> (Vec<Message>, usize, usize, usize) {
    // create a test message
    let redis_url = format!("redis://localhost:{}", redis_port);
    let msg = new_message(cmd, 120, 3, "TestDataTestDataTestDataTestData", twin_dst);
    // duplicate the message
    let messages = std::iter::repeat_with(|| msg.clone())
        .take(msg_count)
        .collect::<Vec<_>>();
    // send the messages to local redis
    let (responses_expected, return_queues) = send_all(messages, &redis_url).await.unwrap();
    // wait on the return queues
    wait_for_responses(
        responses_expected,
        return_queues,
        60, // give up if you didn't get all responses and $timeout seconds have passed since the last response was received
        &redis_url,
    )
    .await
    .unwrap()
}

async fn handle_cmd(cmd: &str, redis_port: usize) -> Result<()> {
    let local_redis = format!("redis://localhost:{}", redis_port);
    let pool = redis::pool(local_redis)
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
    id: u32,
) -> Result<()> {
    let processor_handler = tokio::spawn(processor(id, storage.clone()));

    let api_handler =
        tokio::spawn(HttpApi::new(id, address, storage.clone(), ident.clone(), db.clone())?.run());

    let proxy_handler = tokio::spawn(ProxyWorker::new(id, 10, storage.clone(), db.clone()).run());

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
enum KPair {
    EdPair,
    SrPair,
}

fn create_test_signer(p: KPair) -> Result<impl Signer> {
    match p {
        KPair::EdPair => {
            let (_pair, string, _seed) = EdPair::generate_with_phrase(None);
            let ed_signer = identity::Ed25519Signer::try_from(string.as_ref()).unwrap();
            Ok(ed_signer)
        }
        KPair::SrPair => {
            let (_pair, string, _seed) = SrPair::generate_with_phrase(None);
            let sr_signer = identity::Ed25519Signer::try_from(string.as_ref()).unwrap();
            Ok(sr_signer)
        }
    }
}

async fn create_local_redis_storage(port: usize) -> Result<impl Storage + ProxyStorage> {
    let redis_url = format!("redis://localhost:{}", port);
    let pool = redis::pool(redis_url)
        .await
        .context("unable to create redis connection")?;
    let storage = RedisStorage::builder(pool).build();
    Ok(storage)
}

fn create_mock_db(twins: Vec<&Twin>) -> Result<impl TwinDB + Clone> {
    let mut db = InMemoryDB::default();

    for twin in twins {
        db.add(twin.clone());
    }
    Ok(db)
}

fn start_redis_server(port: usize) -> Result<()> {
    Command::new("sh")
        .arg("-c")
        .arg(format!(
            "docker run --name redis-test-{} --rm -d -p {}:6379 redis",
            port, port
        ))
        .output()
        .expect("failed to execute process");
    Ok(())
}

fn stop_redis_server(port: usize) -> Result<()> {
    Command::new("sh")
        .arg("-c")
        .arg(format!("docker stop redis-test-{}", port))
        .output()
        .expect("failed to execute process");
    Ok(())
}

// RedisManager used here to spawn number of redis processes
// and take care of cleaning up when test is done even in case of panic
struct RedisManager {
    ports: Vec<usize>,
}

impl RedisManager {
    fn init(&self) {
        for port in &self.ports {
            start_redis_server(*port).unwrap();
        }
    }
}

impl Drop for RedisManager {
    fn drop(&mut self) {
        for port in &self.ports {
            stop_redis_server(*port).unwrap();
        }
    }
}

static INIT: Once = Once::new();

pub fn initialize_logger() {
    INIT.call_once(|| {
        simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Warn)
            .with_module_level("hyper", log::LevelFilter::Off)
            .with_module_level("ws", log::LevelFilter::Off)
            .with_module_level("substrate_api_client", log::LevelFilter::Off)
            .init()
            .unwrap();
    });
}

#[tokio::test]
async fn test_message_exchange_with_edpair() {
    initialize_logger();
    let local_redis_port = 6380;
    let remote_redis_port = 6381;
    let t1 = create_test_signer(KPair::EdPair).unwrap();
    let t2 = create_test_signer(KPair::EdPair).unwrap();

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
    // creating mock db with dummy entities
    let db1 = create_mock_db(vec![&twin1, &twin2]).unwrap();
    let db2 = create_mock_db(vec![&twin1, &twin2]).unwrap();

    // start redis servers
    let redis_manager = RedisManager {
        ports: vec![local_redis_port, remote_redis_port],
    };
    redis_manager.init();
    // create redis storage
    let storage1 = create_local_redis_storage(local_redis_port).await.unwrap();
    let storage2 = create_local_redis_storage(remote_redis_port).await.unwrap();

    // start two instances of RMB
    tokio::spawn(async move { start_rmb(db1, storage1, t1, &twin1.address, twin1.id).await });
    tokio::spawn(async move { start_rmb(db2, storage2, t2, &twin2.address, twin2.id).await });
    // mimic a process that handle a command `testme` from a remote node
    let cmd = "testme";
    tokio::spawn(async move { handle_cmd(cmd, remote_redis_port).await.unwrap() });
    // test simple message exchange
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id], 1, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id], 200, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id], 1000, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
}

#[tokio::test]
async fn test_message_exchange_with_srpair() {
    initialize_logger();
    let local_redis_port = 6382;
    let remote_redis_port = 6383;
    let t1 = create_test_signer(KPair::SrPair).unwrap();
    let t2 = create_test_signer(KPair::SrPair).unwrap();

    // create dummy entities for testing
    let twin1: Twin = Twin {
        version: 1,
        id: 1,
        account: t1.account(),
        address: "127.0.0.1:5830".to_string(),
        entities: vec![],
    };
    let twin2: Twin = Twin {
        version: 1,
        id: 2,
        account: t2.account(),
        address: "127.0.0.1:5840".to_string(),
        entities: vec![],
    };
    // creating mock db with dummy entities
    let db1 = create_mock_db(vec![&twin1, &twin2]).unwrap();
    let db2 = create_mock_db(vec![&twin1, &twin2]).unwrap();

    // start redis servers
    let redis_manager = RedisManager {
        ports: vec![local_redis_port, remote_redis_port],
    };
    redis_manager.init();
    // create redis storage
    let storage1 = create_local_redis_storage(local_redis_port).await.unwrap();
    let storage2 = create_local_redis_storage(remote_redis_port).await.unwrap();

    // start two instances of RMB
    tokio::spawn(async move { start_rmb(db1, storage1, t1, &twin1.address, twin1.id).await });
    tokio::spawn(async move { start_rmb(db2, storage2, t2, &twin2.address, twin2.id).await });
    // mimic a process that handle a command `testme` from a remote node
    let cmd = "testme";
    tokio::spawn(async move { handle_cmd(cmd, remote_redis_port).await.unwrap() });
    // test simple message exchange
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id], 1, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id], 200, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id], 1000, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
}

#[tokio::test]
async fn test_multi_dest_message_exchange() {
    initialize_logger();
    let local_redis_port = 6384;
    let remote1_redis_port = 6385;
    let remote2_redis_port = 6386;
    let t1 = create_test_signer(KPair::SrPair).unwrap();
    let t2 = create_test_signer(KPair::SrPair).unwrap();
    let t3 = create_test_signer(KPair::SrPair).unwrap();

    // create dummy entities for testing
    let twin1: Twin = Twin {
        version: 1,
        id: 1,
        account: t1.account(),
        address: "127.0.0.1:5850".to_string(),
        entities: vec![],
    };
    let twin2: Twin = Twin {
        version: 1,
        id: 2,
        account: t2.account(),
        address: "127.0.0.1:5860".to_string(),
        entities: vec![],
    };
    let twin3: Twin = Twin {
        version: 1,
        id: 3,
        account: t3.account(),
        address: "127.0.0.1:5870".to_string(),
        entities: vec![],
    };

    // creating mock db with dummy entities
    let db1 = create_mock_db(vec![&twin1, &twin2, &twin3]).unwrap();
    let db2 = create_mock_db(vec![&twin1, &twin2, &twin3]).unwrap();
    let db3 = create_mock_db(vec![&twin1, &twin2, &twin3]).unwrap();

    // start redis servers
    let redis_manager = RedisManager {
        ports: vec![local_redis_port, remote1_redis_port, remote2_redis_port],
    };
    redis_manager.init();
    // create redis storage
    let storage1 = create_local_redis_storage(local_redis_port).await.unwrap();
    let storage2 = create_local_redis_storage(remote1_redis_port)
        .await
        .unwrap();
    let storage3 = create_local_redis_storage(remote2_redis_port)
        .await
        .unwrap();

    // start three instances of RMB
    tokio::spawn(async move { start_rmb(db1, storage1, t1, &twin1.address, twin1.id).await });
    tokio::spawn(async move { start_rmb(db2, storage2, t2, &twin2.address, twin2.id).await });
    tokio::spawn(async move { start_rmb(db3, storage3, t3, &twin3.address, twin3.id).await });
    // mimic a process that handle a command `testme` from two remote nodes
    let cmd = "testme";
    tokio::spawn(async move { handle_cmd(cmd, remote1_redis_port).await.unwrap() });
    tokio::spawn(async move { handle_cmd(cmd, remote2_redis_port).await.unwrap() });
    // test simple message exchange
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id, twin3.id], 1, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id, twin3.id], 200, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id, twin3.id], 1000, local_redis_port).await;
    assert_eq!(err_count, 0);
    assert_eq!(timed_out_count, 0);
}

#[tokio::test]
async fn test_twin_not_found() {
    initialize_logger();
    let local_redis_port = 6387;
    let t1 = create_test_signer(KPair::SrPair).unwrap();

    // create dummy entities for testing
    let twin1: Twin = Twin {
        version: 1,
        id: 1,
        account: t1.account(),
        address: "127.0.0.1:5880".to_string(),
        entities: vec![],
    };

    // creating mock db with dummy entities
    let db1 = create_mock_db(vec![]).unwrap();

    // start redis servers
    let redis_manager = RedisManager {
        ports: vec![local_redis_port],
    };
    redis_manager.init();
    // create redis storage
    let storage1 = create_local_redis_storage(local_redis_port).await.unwrap();

    // start three instances of RMB
    tokio::spawn(async move { start_rmb(db1, storage1, t1, &twin1.address, twin1.id).await });

    // test getting twin not found as response error
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait("testme", vec![2], 1, local_redis_port).await;
    assert_eq!(err_count, 1);
    assert_eq!(timed_out_count, 0);
    assert!(_responses[0].error.as_ref().unwrap().contains("not found"));
}

#[tokio::test]
async fn test_invalid_dest() {
    initialize_logger();
    let local_redis_port = 6388;
    let remote_redis_port = 6389;
    let t1 = create_test_signer(KPair::SrPair).unwrap();
    let t2 = create_test_signer(KPair::SrPair).unwrap();

    // create dummy entities for testing
    let twin1: Twin = Twin {
        version: 1,
        id: 1,
        account: t1.account(),
        address: "127.0.0.1:5890".to_string(),
        entities: vec![],
    };
    let twin2: Twin = Twin {
        version: 1,
        id: 2,
        account: t2.account(),
        address: "127.0.0.1:5900".to_string(),
        entities: vec![],
    };

    // creating mock db with dummy entities
    let db1 = create_mock_db(vec![&twin2]).unwrap();
    let db2 = create_mock_db(vec![&twin1]).unwrap();

    // start redis servers
    let redis_manager = RedisManager {
        ports: vec![local_redis_port, remote_redis_port],
    };
    redis_manager.init();
    // create redis storage
    let storage1 = create_local_redis_storage(local_redis_port).await.unwrap();
    let storage2 = create_local_redis_storage(remote_redis_port).await.unwrap();

    // start two instances of RMB with id 1, and id 3
    tokio::spawn(async move { start_rmb(db1, storage1, t1, &twin1.address, twin1.id).await });
    tokio::spawn(async move { start_rmb(db2, storage2, t2, &twin2.address, 3).await });
    // mimic a process that handle a command `testme` from a remote nodes
    let cmd = "testme";
    tokio::spawn(async move { handle_cmd(cmd, remote_redis_port).await.unwrap() });
    // test getting bad request as response error
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id], 1, local_redis_port).await;
    assert_eq!(err_count, 1);
    assert_eq!(timed_out_count, 0);
    assert!(_responses[0]
        .error
        .as_ref()
        .unwrap()
        .contains("Bad Request"));
}

#[tokio::test]
async fn test_unauthorized() {
    initialize_logger();
    let local_redis_port = 6390;
    let remote_redis_port = 6391;
    let t1 = create_test_signer(KPair::SrPair).unwrap();
    let t2 = create_test_signer(KPair::SrPair).unwrap();
    let t3 = create_test_signer(KPair::SrPair).unwrap();

    // create dummy entities for testing
    let twin1: Twin = Twin {
        version: 1,
        id: 1,
        account: t3.account(), // unmatched public key
        address: "127.0.0.1:5910".to_string(),
        entities: vec![],
    };
    let twin2: Twin = Twin {
        version: 1,
        id: 2,
        account: t2.account(),
        address: "127.0.0.1:5920".to_string(),
        entities: vec![],
    };

    // creating mock db with dummy entities
    let db1 = create_mock_db(vec![&twin2]).unwrap();
    let db2 = create_mock_db(vec![&twin1]).unwrap();

    // start redis servers
    let redis_manager = RedisManager {
        ports: vec![local_redis_port, remote_redis_port],
    };
    redis_manager.init();
    // create redis storage
    let storage1 = create_local_redis_storage(local_redis_port).await.unwrap();
    let storage2 = create_local_redis_storage(remote_redis_port).await.unwrap();

    // start two instances of RMB
    tokio::spawn(async move { start_rmb(db1, storage1, t1, &twin1.address, twin1.id).await });
    tokio::spawn(async move { start_rmb(db2, storage2, t2, &twin2.address, twin2.id).await });
    // mimic a process that handle a command `testme` from a remote nodes
    let cmd = "testme";
    tokio::spawn(async move { handle_cmd(cmd, remote_redis_port).await.unwrap() });
    // test getting Unauthorized as response error
    let (_responses, err_count, _success_count, timed_out_count) =
        send_and_wait(cmd, vec![twin2.id], 1, local_redis_port).await;
    assert_eq!(err_count, 1);
    assert_eq!(timed_out_count, 0);
    assert!(_responses[0]
        .error
        .as_ref()
        .unwrap()
        .contains("Unauthorized"));
}
