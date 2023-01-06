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

use bb8_redis::{
    bb8::{Pool, RunError},
    redis::{cmd, FromRedisValue, RedisError},
    RedisConnectionManager,
};
use queue::Queue;
pub use session::MessageID;
use session::*;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

use prometheus::{IntCounter, IntGaugeVec, Opts, Registry};

lazy_static::lazy_static! {
    static ref CON_PER_WORKER: IntGaugeVec = IntGaugeVec::new(
        Opts::new("rely_worker_connections", "number of connections handled by this worker"),
        &["worker"]).unwrap();

    static ref MESSAGE_RX: IntCounter = IntCounter::new("rely_message_rx", "number of messages received by rely").unwrap();

    static ref MESSAGE_TX: IntCounter = IntCounter::new("rely_message_tx", "number of messages forwarded by rely").unwrap();
}

pub const DEFAULT_WORKERS: u32 = 100;
pub const DEFAULT_USERS: usize = 100_1000;

const MIN_JOBS_POP: usize = 100;
const READ_COUNT: usize = 100;
const READ_BLOCK_MS: usize = 5000; // 5 seconds
const RETRY_DELAY: Duration = Duration::from_secs(2);
const QUEUE_MAXLEN: usize = 10000;

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

#[async_trait::async_trait]
pub trait Hook: Send + Sync + 'static {
    async fn received<T>(&self, id: MessageID, data: T)
    where
        T: AsRef<[u8]> + Send + Sync;
}

struct User<H>
where
    H: Hook,
{
    connection: ConnectionID,
    hook: H,
}

type UserMap<S> = HashMap<StreamID, User<S>>;

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

    pub async fn build<H: Hook>(self) -> Result<Switch<H>> {
        Switch::new(self).await
    }
}

/// Rely is main rely object
pub struct Switch<H>
where
    H: Hook,
{
    pool: Pool<RedisConnectionManager>,
    users: Arc<RwLock<UserMap<H>>>,
    queue: Queue<Job>,
    max_users: usize,
}

impl<H> Clone for Switch<H>
where
    H: Hook,
{
    fn clone(&self) -> Self {
        Switch {
            pool: self.pool.clone(),
            users: Arc::clone(&self.users),
            queue: self.queue.clone(),
            max_users: self.max_users,
        }
    }
}
impl<H> Switch<H>
where
    H: Hook,
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
        let rely = Switch {
            pool,
            queue,
            users: Arc::new(RwLock::new(UserMap::default())),
            max_users,
        };

        let mut user_per_worker = max_users / (workers as usize);
        if user_per_worker == 0 {
            user_per_worker = 1;
        }

        opts.registry.register(Box::new(CON_PER_WORKER.clone()))?;
        opts.registry.register(Box::new(MESSAGE_RX.clone()))?;
        opts.registry.register(Box::new(MESSAGE_TX.clone()))?;

        for id in 0..workers {
            // TODO: while workers are mostly ideal may be it's better in the
            // future to spawn new workers only when needed (hitting max)
            // number of users per current active workers for example!
            tokio::spawn(rely.clone().worker(id, user_per_worker));
        }

        Ok(rely)
    }

    async fn process(&self, connections: &mut HashMap<StreamID, Connection>) -> Result<()> {
        let users = self.users.read().await;

        connections.retain(|k, v| matches!(users.get(k), Some(u) if &u.connection == v.id()));
        if connections.is_empty() {
            return Ok(());
        }

        drop(users);
        // build command
        let (stream_ids, stream_connections): (Vec<&StreamID>, Vec<&Connection>) =
            connections.iter().unzip();
        //TODO: may be not rebuild the command each time when the list of current users don't change.
        let mut c = cmd("XREAD");
        let mut c = c
            .arg("COUNT")
            .arg(READ_COUNT)
            .arg("BLOCK")
            .arg(READ_BLOCK_MS)
            .arg("STREAMS");

        for id in stream_ids {
            // convert streamID to full stream name (say stream:<id>)
            c = c.arg(id);
        }

        for session in stream_connections {
            c = c.arg(session.last());
        }

        // now actually run the command
        let mut con = self.pool.get().await?;

        let output: Output = c.query_async(&mut *con).await?;

        let output = match output {
            None => return Ok(()),
            Some(streams) => streams,
        };

        let users = self.users.read().await;
        for Messages(stream_id, messages) in output {
            let user = match users.get(&stream_id) {
                Some(user) => user,
                None => {
                    connections.remove(&stream_id);
                    continue;
                }
            };
            let session = connections.get_mut(&stream_id).expect("not possible");
            if &user.connection != session.id() {
                // user has disconnected, and reconnected hence is probably served now
                // by another workers.
                connections.remove(&stream_id);
                continue;
            }
            // now we know that the "connected" user is indeed served by this worker
            // hence we can now feed the messages.
            for Message(msg_id, tags) in messages {
                // tags are always of even length (k, v) but we only right now
                // only use tag _ and value is actual content
                if tags.len() != 2 {
                    log::warn!("received a message with tag count={}", tags.len());
                    continue;
                }

                user.hook.received(msg_id, &tags[1]).await;

                MESSAGE_TX.inc();

                session.set_last(msg_id);
            }
        }

        Ok(())
    }

    async fn worker(self, id: u32, nr: usize) {
        log::trace!("[{}] worker started", id);
        // a worker will wait for available registrations.
        // once registrations are available, it will then maintain it's own list
        // of user ids (in memory) with a
        let mut connections: HashMap<StreamID, Connection> = HashMap::default();
        // wait for the first set of users
        let name = id.to_string();

        CON_PER_WORKER.with_label_values(&[&name]).set(0);

        loop {
            log::trace!("[{}] waiting for connections", id);
            for job in self.queue.pop(min(nr, MIN_JOBS_POP)).await.into_iter() {
                connections.insert(job.0, job.1.into());
            }

            'inner: loop {
                log::trace!("[{}] handling {} connections", id, connections.len());

                CON_PER_WORKER
                    .with_label_values(&[&name])
                    .set(connections.len() as i64);

                // process
                match self.process(&mut connections).await {
                    Ok(_) => {}
                    Err(SwitchError::RedisError(err)) => {
                        log::error!("[{}] error while waiting for messages: {}", id, err);
                        // this delay in case of redis connection error that we
                        // wait before retrying
                        sleep(RETRY_DELAY).await;
                    }
                    Err(err) => {
                        log::error!("[{}] error while waiting for messages: {}", id, err);
                    }
                };

                // no more sessions to serve
                if connections.is_empty() {
                    break 'inner;
                }

                // can we take more users?
                if connections.len() >= nr {
                    // no.
                    continue;
                }

                for job in self
                    .queue
                    .pop_no_wait(min(nr - connections.len(), MIN_JOBS_POP))
                    .await
                    .into_iter()
                {
                    connections.insert(job.0, job.1.into());
                }
            }
        }
    }

    pub async fn register(&self, id: u32, hook: H) -> Result<Handle<H>> {
        // to make sure
        let stream_id: StreamID = id.into();
        let connection_id = ConnectionID::new();

        let user = User {
            connection: connection_id,
            hook,
        };

        let mut map = self.users.write().await;
        if map.len() > self.max_users {
            return Err(SwitchError::MaxNumberOfUsers);
        }
        // this overrides the previous user object. which means workers who
        // has been handling this user connection should forget about him and
        // don't wait on messages for it anymore.
        map.insert(stream_id, user);
        self.queue.push(Job(stream_id, connection_id)).await;
        Ok(Handle {
            id: stream_id,
            m: Arc::clone(&self.users),
        })
    }

    pub async fn ack(&self, id: u32, ids: &[MessageID]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let id: StreamID = id.into();
        let mut con = self.pool.get().await?;
        let mut c = cmd("XDEL");
        let mut c = c.arg(id);
        for id in ids.iter() {
            c = c.arg(id);
        }

        c.query_async(&mut *con).await?;

        Ok(())
    }

    pub async fn send<T: AsRef<[u8]>>(&self, id: u32, msg: T) -> Result<MessageID> {
        let id: StreamID = id.into();
        let mut con = self.pool.get().await?;
        let id: MessageID = cmd("XADD")
            .arg(id)
            .arg("MAXLEN")
            .arg("~")
            .arg(QUEUE_MAXLEN)
            .arg("*")
            .arg("_")
            .arg(msg.as_ref())
            .query_async(&mut *con)
            .await?;

        MESSAGE_RX.inc();

        Ok(id)
    }
}

pub struct Handle<H>
where
    H: Hook,
{
    id: StreamID,
    m: Arc<RwLock<UserMap<H>>>,
}

impl<S> Drop for Handle<S>
where
    S: Hook,
{
    fn drop(&mut self) {
        log::debug!("dropping stream: {}", self.id);
        let m = self.m.clone();
        let id = self.id;
        tokio::spawn(async move {
            let mut m = m.write().await;
            m.remove(&id);
        });
    }
}

use bb8_redis::redis::{ErrorKind, RedisResult, Value};
struct Job(StreamID, ConnectionID);
struct Message(MessageID, Vec<Vec<u8>>);

impl FromRedisValue for Message {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Bulk(values) => {
                if values.len() != 2 {
                    return Err(RedisError::from((
                        ErrorKind::TypeError,
                        "expecting 2 value",
                    )));
                }
                Ok(Self(
                    MessageID::from_redis_value(&values[0])?,
                    Vec::from_redis_value(&values[1])?,
                ))
            }
            _ => Err(RedisError::from((
                ErrorKind::TypeError,
                "expecting a tuple",
            ))),
        }
    }
}

struct Messages(StreamID, Vec<Message>);

impl FromRedisValue for Messages {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Bulk(values) => {
                if values.len() != 2 {
                    return Err(RedisError::from((
                        ErrorKind::TypeError,
                        "expecting 2 value",
                    )));
                }
                Ok(Self(
                    StreamID::from_redis_value(&values[0])?,
                    Vec::from_redis_value(&values[1])?,
                ))
            }
            _ => Err(RedisError::from((
                ErrorKind::TypeError,
                "expecting a tuple",
            ))),
        }
    }
}

type Output = Option<Vec<Messages>>;

#[cfg(test)]
mod test {
    use super::{MessageID, Output, StreamID};
    use bb8_redis::redis::{self, cmd};

    #[test]
    fn message_serialization() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut con = client.get_connection().unwrap();
        let stream = StreamID::from(0);
        let msg = "hello world";
        let _: () = cmd("XADD")
            .arg(stream)
            .arg("MAXLEN")
            .arg(1)
            .arg("*")
            .arg("_")
            .arg(msg)
            .query(&mut con)
            .unwrap();
        let msg_id = MessageID::default();
        let output: Output = cmd("XREAD")
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(stream)
            .arg(msg_id)
            .query(&mut con)
            .unwrap();

        assert!(output.is_some());
        let output = output.unwrap();

        assert_eq!(output.len(), 1);

        let messages = &output[0];
        assert_eq!(messages.0.id(), 0);
        let messages = &messages.1;

        assert_eq!(messages.len(), 1);

        let message = &messages[0];

        let tags = &message.1;
        assert_eq!(tags.len(), 2);
        let v = std::str::from_utf8(&tags[1]).unwrap();
        assert_eq!(v, "hello world");
    }
}
