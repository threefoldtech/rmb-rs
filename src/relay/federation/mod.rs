use std::marker::PhantomData;

use self::router::{Router, RouterError};
use super::{ranker::RelayRanker, switch::Sink};
use crate::twin::TwinDB;
use crate::types::Envelope;
use anyhow::Result;
use bb8_redis::{
    bb8::{Pool, RunError},
    RedisConnectionManager,
};
use futures_util::StreamExt;
use prometheus::{IntCounterVec, Opts, Registry};
use protobuf::Message;
use redis::streams::StreamReadReply;
use redis::{cmd, RedisError, Value};
use workers::Work;

mod router;

lazy_static::lazy_static! {
    static ref MESSAGE_SUCCESS: IntCounterVec = IntCounterVec::new(
        Opts::new("federation_message_success", "number of messages send successfully via federation"),
        &["relay"]).unwrap();

    static ref MESSAGE_ERROR: IntCounterVec = IntCounterVec::new(
        Opts::new("federation_message_error", "number of messages that failed to send via federation"),
        &["relay"]).unwrap();
}

// Constants for readability and maintainability
const MSG_FIELD: &str = "msg";
const RETRY_THROTTLE_SECS: u64 = 2;
const CLAIM_MIN_IDLE_MS: i64 = 3_000; // 3 seconds
const XPENDING_COUNT: i64 = 200;
const XREAD_COUNT: i64 = 100;
const XREAD_BLOCK_MS: usize = 500;
// Limit per-consumer parallelism; keep conservative by default
const PARALLEL_PER_CONSUMER: usize = 4;

async fn handle_xgroup_errors<C>(con: &mut C, consumer_id: &str, s: &str) -> bool
where
    C: redis::aio::ConnectionLike + Send,
{
    if s.contains("NOGROUP") {
        if let Err(e) = cmd("XGROUP")
            .arg("CREATE")
            .arg(FEDERATION_STREAM)
            .arg(FEDERATION_GROUP)
            .arg("0")
            .arg("MKSTREAM")
            .query_async::<String>(con)
            .await
        {
            log::error!("XGROUP CREATE failed: {}", e);
        }
        true
    } else if s.contains("NOCONSUMER") {
        if let Err(e) = cmd("XGROUP")
            .arg("CREATECONSUMER")
            .arg(FEDERATION_STREAM)
            .arg(FEDERATION_GROUP)
            .arg(consumer_id)
            .query_async::<String>(con)
            .await
        {
            log::error!("XGROUP CREATECONSUMER failed: {}", e);
        }
        true
    } else {
        false
    }
}

#[derive(Clone)]
struct StreamWorker<D: TwinDB> {
    pool: Pool<RedisConnectionManager>,
    router: Router<D>,
}

impl<D: TwinDB> StreamWorker<D> {
    fn new(pool: Pool<RedisConnectionManager>, router: Router<D>) -> Self {
        Self { pool, router }
    }
}

struct StreamJob {
    entry_id: String,
    msg: Vec<u8>,
}

#[async_trait::async_trait]
impl<D> workers::Work for StreamWorker<D>
where
    D: TwinDB,
{
    type Input = StreamJob;
    type Output = ();

    async fn run(&mut self, job: Self::Input) {
        // Try process
        match self.router.process(job.msg.clone()).await {
            Ok(_) => {
                if let Ok(mut con) = self.pool.get().await {
                    if let Err(e) = cmd("XACK")
                        .arg(FEDERATION_STREAM)
                        .arg(FEDERATION_GROUP)
                        .arg(&job.entry_id)
                        .query_async::<i32>(&mut *con)
                        .await
                    {
                        log::error!("Failed to XACK message {}: {}", job.entry_id, e);
                    }
                }
            }
            Err(err) => {
                // Decide based on typed RouterError
                let should_ack = matches!(err, RouterError::Permanent(_) | RouterError::Expired);
                if should_ack {
                    // For permanent/expired, send error response once, then ACK
                    let mut env = Envelope::new();
                    if env.merge_from_bytes(&job.msg).is_ok() {
                        self.router
                            .send_error_response(&env, &err.to_string())
                            .await;
                    }
                    // Ack permanently failed or expired messages
                    if let Ok(mut con) = self.pool.get().await {
                        if let Err(e) = cmd("XACK")
                            .arg(FEDERATION_STREAM)
                            .arg(FEDERATION_GROUP)
                            .arg(&job.entry_id)
                            .query_async::<i32>(&mut *con)
                            .await
                        {
                            log::error!("Failed to XACK message {}: {}", job.entry_id, e);
                        }
                    }
                } else {
                    // Leave unacked for PEL monitor to claim later
                    log::debug!("leaving entry {} pending for retry via PEL", job.entry_id);
                }
            }
        }
    }
}

// --- Helpers ---

fn extract_pending_ids(val: &Value, min_idle_ms: i64) -> Vec<String> {
    // XPENDING reply rows: [id, consumer, idle, deliveries]
    match val {
        Value::Array(rows) => rows
            .iter()
            .filter_map(|row| match row {
                Value::Array(cols) if cols.len() >= 3 => match (&cols[0], &cols[2]) {
                    (Value::BulkString(id_bs), Value::Int(idle)) if *idle >= min_idle_ms => {
                        Some(String::from_utf8_lossy(id_bs).to_string())
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn extract_claimed_msgs(val: &Value) -> Vec<(String, Vec<u8>)> {
    // Manual decode: XCLAIM returns array of entries: [id, [field, value, ...]]
    match val {
        Value::Array(entries) => entries
            .iter()
            .filter_map(|ent| match ent {
                Value::Array(parts) if parts.len() >= 2 => match (&parts[0], &parts[1]) {
                    (Value::BulkString(id_bs), Value::Array(kvs)) => {
                        let entry_id = String::from_utf8_lossy(id_bs).to_string();
                        kvs.chunks_exact(2)
                            .find_map(|chunk| match (&chunk[0], &chunk[1]) {
                                (Value::BulkString(k), Value::BulkString(v))
                                    if k == MSG_FIELD.as_bytes() =>
                                {
                                    Some((entry_id.clone(), v.clone()))
                                }
                                _ => None,
                            })
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub const DEFAULT_WORKERS: usize = 100;
// Streams-based ingest
pub const FEDERATION_STREAM: &str = "relay.federation.stream";
pub const FEDERATION_GROUP: &str = "federation";
pub const FEDERATION_CONSUMER: &str = "relay";
// Retry structures
// No custom retry keys when using Streams PEL

#[derive(thiserror::Error, Debug)]
pub enum FederationError {
    #[error("pool error: {0}")]
    PoolError(#[from] RunError<RedisError>),
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),
}

pub struct FederationOptions<D: TwinDB> {
    pool: Pool<RedisConnectionManager>,
    workers: usize,
    registry: Registry,
    _marker: PhantomData<D>,
}

impl<D> FederationOptions<D>
where
    D: TwinDB + Clone,
{
    pub fn new(pool: Pool<RedisConnectionManager>) -> Self {
        Self {
            pool,
            workers: DEFAULT_WORKERS,
            registry: prometheus::default_registry().clone(),
            _marker: PhantomData,
        }
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    pub(crate) fn build(self, sink: Sink, twins: D, ranker: RelayRanker) -> Result<Federation<D>> {
        Federation::new(self, sink, twins, ranker)
    }
}

pub struct Federation<D: TwinDB> {
    pool: Pool<RedisConnectionManager>,
    router: Router<D>,
    worker_count: usize,
}

#[derive(Clone)]
pub struct Federator {
    pool: Pool<RedisConnectionManager>,
}

impl Federator {
    // Sends federation msg to redis on (relay.federation)
    pub async fn send<T: AsRef<[u8]>>(&self, msg: T) -> Result<(), FederationError> {
        let mut con = self.pool.get().await?;
        // XADD stream * msg <bytes>
        cmd("XADD")
            .arg(FEDERATION_STREAM)
            .arg("*")
            .arg("msg")
            .arg(msg.as_ref())
            .query_async::<String>(&mut *con)
            .await?;

        Ok(())
    }
}

impl<D> Federation<D>
where
    D: TwinDB,
{
    /// create a new federation router
    fn new(opts: FederationOptions<D>, sink: Sink, twins: D, ranker: RelayRanker) -> Result<Self> {
        opts.registry.register(Box::new(MESSAGE_SUCCESS.clone()))?;
        opts.registry.register(Box::new(MESSAGE_ERROR.clone()))?;

        let runner = Router::new(sink, twins, ranker);
        Ok(Self {
            pool: opts.pool,
            router: runner,
            worker_count: opts.workers,
        })
    }

    /// start the federation router
    pub fn start(self) -> Federator {
        let federator = Federator {
            pool: self.pool.clone(),
        };
        // Spawn per-consumer reader loops (unique consumer id per loop)
        for i in 0..self.worker_count {
            let pool = self.pool.clone();
            let worker = StreamWorker::new(pool.clone(), self.router.clone());
            let consumer_id = format!("{}-{}", FEDERATION_CONSUMER, i);
            tokio::spawn(async move {
                per_consumer_loop::<D>(pool, worker, consumer_id).await;
            });
        }
        federator
    }
}

async fn per_consumer_loop<D: TwinDB>(
    pool: Pool<RedisConnectionManager>,
    worker: StreamWorker<D>,
    consumer_id: String,
) {
    use tokio::time::{Duration, Instant};
    let mut last_retry = Instant::now() - Duration::from_secs(RETRY_THROTTLE_SECS);
    loop {
        // Retry: scan this consumer's PEL and claim old entries (>= CLAIM_MIN_IDLE_MS)
        if let Ok(mut con) = pool.get().await {
            // Throttle retries by interval (RETRY_THROTTLE_SECS)
            if last_retry.elapsed() >= Duration::from_secs(RETRY_THROTTLE_SECS) {
                match cmd("XPENDING")
                    .arg(FEDERATION_STREAM)
                    .arg(FEDERATION_GROUP)
                    .arg("-")
                    .arg("+")
                    .arg(XPENDING_COUNT)
                    .arg(&consumer_id)
                    .query_async::<Value>(&mut *con)
                    .await
                {
                    Ok(v) => {
                        let to_claim = extract_pending_ids(&v, CLAIM_MIN_IDLE_MS);
                        if !to_claim.is_empty() {
                            let mut c = cmd("XCLAIM");
                            c.arg(FEDERATION_STREAM)
                                .arg(FEDERATION_GROUP)
                                .arg(&consumer_id)
                                .arg(CLAIM_MIN_IDLE_MS)
                                .arg(to_claim.as_slice());
                            match c.query_async::<Value>(&mut *con).await {
                                Ok(v) => {
                                    // Process claimed messages with bounded concurrency
                                    let claimed: Vec<(String, Vec<u8>)> = extract_claimed_msgs(&v);
                                    let parallel = PARALLEL_PER_CONSUMER;
                                    let stream = futures::stream::iter(claimed.into_iter().map(
                                        |(entry_id, msg)| {
                                            let mut w = worker.clone();
                                            async move { w.run(StreamJob { entry_id, msg }).await }
                                        },
                                    ))
                                    .buffer_unordered(parallel);
                                    futures::pin_mut!(stream);
                                    while (stream.next().await).is_some() {}
                                }
                                Err(e) => {
                                    let s = e.to_string();
                                    if !handle_xgroup_errors(&mut *con, &consumer_id, &s).await {
                                        log::error!("XCLAIM error: {}", s);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let s = e.to_string();
                        if !handle_xgroup_errors(&mut *con, &consumer_id, &s).await {
                            log::error!("XPENDING error: {}", s);
                        }
                    }
                }
                last_retry = Instant::now();
            }
        }

        // Read new messages for this consumer
        let mut con = match pool.get().await {
            Ok(con) => con,
            Err(err) => {
                log::error!("failed to get redis connection: {}", err);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        };
        let val: Result<StreamReadReply, _> = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(FEDERATION_GROUP)
            .arg(&consumer_id)
            .arg("COUNT")
            .arg(XREAD_COUNT)
            .arg("BLOCK")
            .arg(XREAD_BLOCK_MS)
            .arg("STREAMS")
            .arg(FEDERATION_STREAM)
            .arg(">")
            .query_async(&mut *con)
            .await;
        match val {
            Ok(reply) => {
                if reply.keys.is_empty() {
                    continue;
                }
                // Build jobs then process with bounded concurrency
                let mut jobs = Vec::new();
                for key in &reply.keys {
                    for entry in &key.ids {
                        // Extract msg field
                        if let Some(Value::BulkString(data)) = entry.map.get(MSG_FIELD) {
                            jobs.push(StreamJob {
                                entry_id: entry.id.clone(),
                                msg: data.clone(),
                            });
                        }
                    }
                }
                let parallel = PARALLEL_PER_CONSUMER;
                let stream = futures::stream::iter(jobs.into_iter().map(|job| {
                    let mut w = worker.clone();
                    async move { w.run(job).await }
                }))
                .buffer_unordered(parallel);
                futures::pin_mut!(stream);
                while (stream.next().await).is_some() {}
            }
            Err(err) => {
                let s = err.to_string();
                if !handle_xgroup_errors(&mut *con, &consumer_id, &s).await {
                    log::error!("XREADGROUP error: {}", s);
                }
                continue;
            }
        }
    }
}

// --- Helpers ---

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{
        cache::{Cache, MemCache},
        relay::{ranker::RelayRanker, switch::Sink},
        twin::{RelayDomains, SubstrateTwinDB, Twin},
    };
    use protobuf::Message;
    use subxt::utils::AccountId32;

    use super::*;
    use crate::{
        redis,
        types::{Envelope, EnvelopeExt},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_router() {
        use httpmock::prelude::*;
        let server = MockServer::start();
        let reg = prometheus::Registry::new();
        let pool = redis::pool("redis://localhost:6379", 10).await.unwrap();
        let sink = Sink::new(pool.clone());

        // Cleanup: ensure no leftover stream/group from previous runs
        {
            let mut con = pool.get().await.unwrap();
            // Attempt to destroy the consumer group; ignore errors if it doesn't exist
            let _: Result<String, _> = cmd("XGROUP")
                .arg("DESTROY")
                .arg(FEDERATION_STREAM)
                .arg(FEDERATION_GROUP)
                .query_async(&mut *con)
                .await;
            // Delete the stream key; ignore errors if it doesn't exist
            let _: Result<i32, _> = cmd("DEL")
                .arg(FEDERATION_STREAM)
                .query_async(&mut *con)
                .await;
        }

        let mem: MemCache<Twin> = MemCache::default();
        let account_id: AccountId32 = "5EyHmbLydxX7hXTX7gQqftCJr2e57Z3VNtgd6uxJzZsAjcPb"
            .parse()
            .unwrap();
        let twin_id = 1;
        let twin = Twin {
            id: twin_id.into(),
            account: account_id,
            relay: Some(RelayDomains::new(&[server.address().to_string()])),
            pk: None,
        };
        let _ = mem.set(1, twin.clone()).await;
        let db = SubstrateTwinDB::new(
            vec![String::from("wss://tfchain.dev.grid.tf:443")],
            Some(mem.clone()),
        )
        .await
        .unwrap();
        let ranker = RelayRanker::new(Duration::from_secs(3600));
        let federation = FederationOptions::new(pool)
            .with_registry(reg)
            .with_workers(10)
            .build(sink, db, ranker)
            .unwrap();

        let federator = federation.start();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(202)
                .header("content-type", "text/html")
                .body("ohi");
        });

        for _ in 0..10 {
            let mut env = Envelope::new();
            env.tags = None;
            env.signature = None;
            env.schema = None;
            env.destination = Some(twin_id.into()).into();
            // Ensure TTL is non-zero; Router::process() rejects expired messages
            env.expiration = 60;
            env.stamp();
            let msg = env.write_to_bytes().unwrap();
            federator.send(msg).await.unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        mock.assert_hits(10);
    }
}
