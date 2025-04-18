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
pub use session::{MessageID, SessionID};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::Duration;
use tokio_util::sync::{CancellationToken, DropGuard};
use worker::WorkerJob;

use prometheus::{IntCounter, IntGaugeVec, Opts, Registry};

#[cfg(feature = "tracker")]
use prometheus::IntCounterVec;

lazy_static::lazy_static! {
    static ref CON_PER_WORKER: IntGaugeVec = IntGaugeVec::new(
        Opts::new("relay_worker_connections", "number of connections handled by this worker"),
        &["worker"]).unwrap();


    static ref MESSAGE_RX: IntCounter = IntCounter::new("relay_message_rx", "number of messages received by relay").unwrap();

    static ref MESSAGE_TX: IntCounter = IntCounter::new("relay_message_tx", "number of messages forwarded by relay").unwrap();

    static ref MESSAGE_RX_BYTES: IntCounter = IntCounter::new("relay_message_rx_bytes", "size of messages received by relay in bytes").unwrap();

    static ref MESSAGE_TX_BYTES: IntCounter = IntCounter::new("relay_message_tx_bytes", "size of messages forwarded by relay in bytes").unwrap();
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
const QUEUE_EXPIRE: usize = 3600; // queues can live max of 1 hour

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
    fn send(&self, id: MessageID, data: Vec<u8>) -> std::result::Result<(), SendError>;
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

    pub async fn build<H: ConnectionSender>(self) -> Result<Switch<H>> {
        Switch::new(self).await
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
    async fn new(opts: SwitchOptions) -> Result<Self> {
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

        let mut user_per_worker = max_users / (workers as usize);
        if user_per_worker == 0 {
            user_per_worker = 1;
        }

        opts.registry.register(Box::new(CON_PER_WORKER.clone()))?;
        opts.registry.register(Box::new(MESSAGE_RX.clone()))?;
        opts.registry.register(Box::new(MESSAGE_TX.clone()))?;
        opts.registry.register(Box::new(MESSAGE_RX_BYTES.clone()))?;
        opts.registry.register(Box::new(MESSAGE_TX_BYTES.clone()))?;
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

async fn send(
    stream_id: &SessionID,
    pool: &Pool<RedisConnectionManager>,
    msg: &[u8],
) -> Result<MessageID> {
    let mut con = pool.get().await?;
    let msg_id: MessageID = cmd("XADD")
        .arg(stream_id)
        .arg("MAXLEN")
        .arg("~")
        .arg(QUEUE_MAXLEN)
        .arg("*")
        .arg("_")
        .arg(msg.as_ref())
        .query_async(&mut *con)
        .await?;

    let _: () = cmd("EXPIRE")
        .arg(stream_id)
        .arg(QUEUE_EXPIRE)
        .query_async(&mut *con)
        .await?;

    MESSAGE_RX.inc();
    MESSAGE_RX_BYTES.inc_by(msg.len() as u64);

    Ok(msg_id)
}
