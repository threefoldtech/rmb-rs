/// Rely is the heart of the iris messaging service rely does not care about the
/// content of the message but only the routing (from sender to receiver) using
/// simple redis streams.
///
/// the only thing that rely is trying to accomplish it to reduce the number of
/// connections to the redis backend by reading messages from multiple streams
/// in the same command over multiple workers. In other words a single worker
/// should serve multiple user connections (to receive messages).
/// This way we can limit the number of redis connections open to the backend
/// hence serve more users with a single instance
///
mod queue;
mod session;
mod worker;

use bb8_redis::{
    bb8::{Pool, RunError},
    redis::{cmd, RedisError},
    RedisConnectionManager,
};
use queue::Queue;
use rand::Rng;
pub use session::{MessageID, SessionID};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::Duration;
use tokio_util::sync::{CancellationToken, DropGuard};
use worker::WorkerJob;

use prometheus::{IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry};

lazy_static::lazy_static! {
    static ref CON_PER_WORKER: IntGaugeVec = IntGaugeVec::new(
        Opts::new("relay_worker_connections", "number of connections handled by this worker"),
        &["worker"]).unwrap();


    static ref MESSAGE_RX: IntCounter = IntCounter::new("relay_message_rx", "number of messages received by relay").unwrap();

    static ref MESSAGE_TX: IntCounter = IntCounter::new("relay_message_tx", "number of messages forwarded by relay").unwrap();

    static ref MESSAGE_RX_BYTES: IntCounter = IntCounter::new("relay_message_rx_bytes", "size of messages received by relay in bytes").unwrap();

    static ref MESSAGE_TX_BYTES: IntCounter = IntCounter::new("relay_message_tx_bytes", "size of messages forwarded by relay in bytes").unwrap();

    // Total number of session evictions, labeled by reason
    // reason ∈ { "closed", "back_pressure_timeout", "cancelled" }
    static ref RELAY_SESSION_EVICTIONS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "relay_session_evictions_total",
            "Total number of session evictions from the relay, labeled by reason",
        ),
        &["reason"],
    ).unwrap();
}

#[cfg(feature = "tracker")]
lazy_static::lazy_static! {
    pub static ref MESSAGE_RX_TWIN: IntCounterVec = IntCounterVec::new(
        Opts::new("relay_message_rx_twin", "number of messages received by relay per twin"),
        &["twin"]).unwrap();

}
pub const DEFAULT_WORKERS: u32 = 100;
pub const DEFAULT_USERS: usize = 100_1000;

const READ_COUNT: usize = 100;
const RETRY_DELAY: Duration = Duration::from_secs(2);
const QUEUE_MAXLEN: usize = 10000;
const QUEUE_EXPIRE: usize = 1800; // 30 minutes (matches MAX_TTL)
const MAX_TTL_MS: u64 = 1800 * 1000; // 30 minutes in milliseconds
const XTRIM_SAMPLE_RATE: u32 = 10; // 10% of sends trigger XTRIM

#[derive(thiserror::Error, Debug)]
pub enum SwitchError {
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),

    #[error("max number of users")]
    MaxNumberOfUsers,

    #[error("timeout")]
    TimeOut,

    #[error("prometheus error: {0}")]
    PrometheusError(#[from] prometheus::Error),
}

impl From<RunError<RedisError>> for SwitchError {
    fn from(value: RunError<RedisError>) -> Self {
        match value {
            RunError::User(err) => err.into(),
            RunError::TimedOut => SwitchError::TimeOut,
        }
    }
}

type Result<T> = std::result::Result<T, SwitchError>;

#[derive(thiserror::Error, Debug)]
#[error("callback error")]
pub enum SendError {
    /// Closed is returned if the callback
    /// will never be able to handle any more messages anymore
    #[error("Sender is closed")]
    Closed,
    /// NotEnoughCapacity is returned if callback can't keep up
    /// with messages, but might succeed later
    #[error("Sender has no enough capacity")]
    NotEnoughCapacity,
}

pub trait ConnectionSender: Send + Sync + 'static {
    fn send(&mut self, id: MessageID, data: Vec<u8>) -> std::result::Result<(), SendError>;
    fn can_send(&self) -> bool;
}

type ActiveSessions = HashMap<SessionID, DropGuard>;

pub struct SwitchOptions {
    pool: Pool<RedisConnectionManager>,
    workers: u32,
    max_users: usize,

    registry: Registry,
}
impl SwitchOptions {
    pub fn new(pool: Pool<RedisConnectionManager>) -> Self {
        Self {
            pool,
            workers: DEFAULT_WORKERS,
            max_users: DEFAULT_USERS,
            registry: prometheus::default_registry().clone(),
        }
    }
}

impl SwitchOptions {
    /// Sets max number of users, default to
    pub fn with_workers(mut self, workers: u32) -> Self {
        self.workers = workers;
        self
    }

    pub fn with_max_users(mut self, users: usize) -> Self {
        self.max_users = users;
        self
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    pub fn build<H: ConnectionSender>(self) -> Result<Switch<H>> {
        Switch::new(self)
    }
}

/// Rely is main rely object
pub struct Switch<S>
where
    S: ConnectionSender,
{
    capacity: Arc<Semaphore>,
    queue: Queue<WorkerJob<S>>,
    pool: Pool<RedisConnectionManager>,
    sessions: Arc<Mutex<ActiveSessions>>,
}

impl<S> Switch<S>
where
    S: ConnectionSender,
{
    /// create a new instance of the switch with given redis connection info and
    /// pool size. The pool size determine the number of workers who are pulling
    /// from messages from redis, so we at least have this max amount of active
    /// connections to redis.
    /// workers, is the number of workers to route messages. this is corresponding
    /// also to the max number of redis connections to the redis backend.
    /// Each worker will be responsible for a number of users (to forward their
    /// messages).
    /// max_users, is the max number of users the rely should handle. This can be a
    /// high number, but it also means that for a worker will handle (worst case
    /// if server is full) users/workers number of users.
    ///
    /// Hence the workers/users number must be sane values take in consideration
    /// max number of connections that this server can handle, number of cpus, etc
    fn new(opts: SwitchOptions) -> Result<Self> {
        // TODO: use builder pattern to support setting of the metrics.
        let workers = opts.workers;
        let max_users = opts.max_users;
        let pool = opts.pool;

        let queue = Queue::new();
        let switch = Switch {
            capacity: Arc::new(Semaphore::new(max_users)),
            queue,
            pool,
            sessions: Arc::new(Mutex::new(ActiveSessions::default())),
        };

        // Determine per-worker capacity. If workers == 0 (tests may spawn manually), avoid div-by-zero.
        let user_per_worker = if workers == 0 {
            1
        } else {
            (max_users / workers as usize).max(1)
        };

        opts.registry.register(Box::new(CON_PER_WORKER.clone()))?;
        opts.registry.register(Box::new(MESSAGE_RX.clone()))?;
        opts.registry.register(Box::new(MESSAGE_TX.clone()))?;
        opts.registry.register(Box::new(MESSAGE_RX_BYTES.clone()))?;
        opts.registry.register(Box::new(MESSAGE_TX_BYTES.clone()))?;
        opts.registry
            .register(Box::new(RELAY_SESSION_EVICTIONS_TOTAL.clone()))?;
        #[cfg(feature = "tracker")]
        opts.registry.register(Box::new(MESSAGE_RX_TWIN.clone()))?;

        for id in 0..workers {
            // TODO: while workers are mostly ideal may be it's better in the
            // future to spawn new workers only when needed (hitting max)
            // number of users per current active workers for example!
            tokio::spawn(worker::Worker::new(id, user_per_worker, &switch).start());
        }

        Ok(switch)
    }

    pub async fn register(
        &self,
        session_id: SessionID,
        cancellation: CancellationToken,
        callback: S,
    ) -> Result<()> {
        let mut sessions = self.sessions.lock().await;
        let Ok(permit) = Arc::clone(&self.capacity).try_acquire_owned() else {
            return Err(SwitchError::MaxNumberOfUsers);
        };

        // this overrides the previous user object. which means workers who
        // has been handling this user connection should forget about him and
        // don't wait on messages for it anymore.

        sessions.insert(session_id.clone(), cancellation.clone().drop_guard());

        self.queue
            .push(WorkerJob::new(session_id, permit, callback, cancellation))
            .await;

        Ok(())
    }

    pub async fn unregister(&self, stream_id: SessionID) {
        let mut sessions = self.sessions.lock().await;
        sessions.remove(&stream_id);
    }

    pub async fn ack<ID: AsRef<SessionID>>(&self, id: ID, ids: &[MessageID]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let id = id.as_ref();
        let mut con = self.pool.get().await?;
        let mut c = cmd("XDEL");
        let mut c = c.arg(id);
        for id in ids.iter() {
            c = c.arg(id);
        }

        let _: () = c.query_async(&mut *con).await?;

        Ok(())
    }

    /// sink returns a clone-able sender if access to the entire switch
    /// need to be limited
    pub fn sink(&self) -> Sink {
        Sink::new(self.pool.clone())
    }

    /// send a message to given ID
    pub async fn send<ID: AsRef<SessionID>, T: AsRef<[u8]>>(
        &self,
        id: ID,
        msg: T,
    ) -> Result<MessageID> {
        send(id.as_ref(), &self.pool, msg.as_ref()).await
    }

    /// checks if a session is connected locally
    /// it doesn't verify if the twin is belongs to this relay
    /// so should be treated as cheap way to route to locals online twins bypassing twin lookup
    /// if the session is not found, further routing verification should be done
    pub async fn is_local(&self, id: &SessionID) -> bool {
        self.sessions.lock().await.contains_key(id)
    }
}

#[derive(Clone)]
pub struct Sink {
    pool: Pool<RedisConnectionManager>,
}

impl Sink {
    pub(crate) fn new(pool: Pool<RedisConnectionManager>) -> Self {
        Self { pool }
    }

    pub async fn send<ID: AsRef<SessionID>, T: AsRef<[u8]>>(
        &self,
        id: ID,
        msg: T,
    ) -> Result<MessageID> {
        send(id.as_ref(), &self.pool, msg.as_ref()).await
    }
}

/// A helper function to decide if a probabilistic action should be triggered.
fn passes_chance(sample_rate: u32) -> bool {
    // If sample_rate is 10, this creates a 10% chance of being true.
    rand::thread_rng().gen_range(0..100) < sample_rate
}

async fn send(
    stream_id: &SessionID,
    pool: &Pool<RedisConnectionManager>,
    msg: &[u8],
) -> Result<MessageID> {
    let mut con = pool.get().await?;

    // Build XADD command
    let mut xadd = cmd("XADD");
    xadd.arg(stream_id)
        .arg("MAXLEN")
        .arg("~")
        .arg(QUEUE_MAXLEN)
        .arg("*")
        .arg("_")
        .arg(msg.as_ref());

    // Pipeline XADD + EXPIRE in a single round-trip (atomic)
    let mut pipe = bb8_redis::redis::pipe();
    pipe.atomic();

    let (msg_id, _): (MessageID, ()) = pipe
        .add_command(xadd)
        .cmd("EXPIRE")
        .arg(stream_id)
        .arg(QUEUE_EXPIRE)
        .query_async(&mut *con)
        .await?;

    // The cleanup strategy is multi-layered but it's necessary to ensure
    // the system doesn't grow indefinitely.
    //
    // Without both EXPIRE and XTRIM, the system would be incomplete
    // EXPIRE alone can't clean active streams, and XTRIM alone wouldn't remove streams for clients that have disconnected permanently.
    // so basically:
    // - EXPIRE: It's the garbage collector for abandoned or idle streams.
    // - XTRIM MINID: It's the housekeeper for busy streams, preventing them from becoming bloated with expired messages.
    //
    // Probabilistic time-based trimming (10% of sends)
    // This remove messages older than MAX_TTL_MS which is the max time allowed for a messages in transit.
    // running XTRIM on every send would needlessly tax the system for no meaningful gain.
    // The 10% probabilistic approach remains superior, automatically adapts, moore sends = more XTRIM
    if passes_chance(XTRIM_SAMPLE_RATE) {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let cutoff_ms = now_ms.saturating_sub(MAX_TTL_MS);

        // XTRIM is best-effort; don't fail send if it errors
        if let Err(e) = cmd("XTRIM")
            .arg(stream_id)
            .arg("MINID")
            .arg(cutoff_ms)
            .query_async::<i64>(&mut *con)
            .await
        {
            log::debug!("XTRIM failed for {}: {}", stream_id, e);
        }
    }

    MESSAGE_RX.inc();
    MESSAGE_RX_BYTES.inc_by(msg.len() as u64);

    Ok(msg_id)
}

#[cfg(test)]
mod test {

    use prometheus::Registry;
    use tokio::sync::mpsc::{self, UnboundedSender};
    use tokio_util::sync::CancellationToken;

    use crate::{redis, relay::switch::SessionID};

    use super::{passes_chance, ConnectionSender, MessageID, Switch};

    #[derive(Clone)]
    struct MessageSender {
        session_id: SessionID,
        tx: mpsc::UnboundedSender<(SessionID, MessageID, String)>,
    }

    impl MessageSender {
        fn new(session_id: SessionID, tx: UnboundedSender<(SessionID, MessageID, String)>) -> Self {
            Self { session_id, tx }
        }
    }

    impl ConnectionSender for MessageSender {
        fn can_send(&self) -> bool {
            true
        }
        fn send(
            &mut self,
            id: super::MessageID,
            data: Vec<u8>,
        ) -> std::result::Result<(), super::SendError> {
            _ = self.tx.send((
                self.session_id.clone(),
                id,
                String::from_utf8_lossy(&data).into(),
            ));

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_switch() {
        // TODO:
        // this is a basic test structure but can be extended and reused to introduce more test cases

        let pool = redis::pool("redis://localhost:6379", 10)
            .await
            .expect("can connect");

        let switch = Switch::<MessageSender>::new(super::SwitchOptions {
            pool,
            workers: 2,
            max_users: 100,
            registry: Registry::new(),
        })
        .expect("switch created");

        let (tx, mut receiver) = mpsc::unbounded_channel();

        // Unique suffix per test run to isolate Redis streams between runs
        let unique_suffix = format!("test-{}", std::process::id());

        // Keep a list of sessions to unregister at the end
        let mut sessions: Vec<SessionID> = Vec::new();

        for id in 1..=100 {
            let session_id = SessionID::new(id.into(), Some(unique_suffix.clone()));
            let handler = MessageSender::new(session_id.clone(), tx.clone());

            switch
                .register(session_id.clone(), CancellationToken::new(), handler)
                .await
                .expect("session registered");

            sessions.push(session_id);
        }

        drop(tx);

        for id in 1..=100 {
            let session_id = SessionID::new(id.into(), Some(unique_suffix.clone()));
            for _ in 0..10 {
                switch
                    .send(&session_id, format!("Hello {id}"))
                    .await
                    .expect("can send");
            }
        }

        let expected_count = 100 * 10;

        let mut count = 0;
        let total = expected_count;
        use tokio::time::{timeout, Duration};
        while count < total {
            match timeout(Duration::from_secs(10), receiver.recv()).await {
                Ok(Some((session_id, msg_id, _msg))) => {
                    switch.ack(&session_id, &[msg_id]).await.expect("ack");
                    count += 1;
                }
                Ok(None) => break, // channel closed unexpectedly
                Err(_) => break,   // timeout
            }
        }

        // Now unregister all sessions to clean up
        for s in sessions {
            switch.unregister(s).await;
        }

        assert_eq!(count, expected_count);
    }

    #[test]
    fn test_passes_chance_probability() {
        const TOTAL_RUNS: u32 = 100_000;
        const SAMPLE_RATE: u32 = 10; // 10%
        let mut true_count = 0;

        for _ in 0..TOTAL_RUNS {
            if passes_chance(SAMPLE_RATE) {
                true_count += 1;
            }
        }

        let actual_percentage = (true_count as f64 / TOTAL_RUNS as f64) * 100.0;
        let expected_percentage = SAMPLE_RATE as f64;

        // We assert that the actual percentage is within a reasonable margin (e.g., 2%)
        // of the expected percentage. This avoids flaky tests due to randomness.
        let margin = 2.0;
        assert!(
            (actual_percentage - expected_percentage).abs() < margin,
            "Actual percentage {:.2}% was not within {:.2}% of expected {}%",
            actual_percentage,
            margin,
            expected_percentage
        );
    }

    #[test]
    fn test_passes_chance_scales_with_activity() {
        const ROUNDS: u32 = 10000;
        const HIGH_ACTIVITY_PER_ROUND: u32 = 7;
        const LOW_ACTIVITY_PER_ROUND: u32 = 3;
        const SAMPLE_RATE: u32 = 10; // 10%

        let mut high_activity_hits = 0;
        let mut low_activity_hits = 0;

        for _ in 0..ROUNDS {
            // In each round, simulate 7 high-activity and 3 low-activity sends
            for _ in 0..HIGH_ACTIVITY_PER_ROUND {
                if passes_chance(SAMPLE_RATE) {
                    high_activity_hits += 1;
                }
            }
            for _ in 0..LOW_ACTIVITY_PER_ROUND {
                if passes_chance(SAMPLE_RATE) {
                    low_activity_hits += 1;
                }
            }
        }

        let total_hits = high_activity_hits + low_activity_hits;
        // Avoid division by zero if no hits occurred (very unlikely but possible)
        if total_hits == 0 {
            return;
        }

        let high_activity_proportion = high_activity_hits as f64 / total_hits as f64;

        // The high-activity group was responsible for 70% of the sends (70000 out of 100000).
        // We expect it to receive roughly 70% of the cleanup triggers.
        let expected_proportion = 0.7;
        let margin = 0.05; // Allow a 5% margin for randomness

        assert!(
            (high_activity_proportion - expected_proportion).abs() < margin,
            "High-activity group received {:.2}% of cleanups, expected around {:.2}%",
            high_activity_proportion * 100.0,
            expected_proportion * 100.0
        );
    }
}
