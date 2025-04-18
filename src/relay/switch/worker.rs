use std::{
    collections::HashMap,
    ops::DerefMut,
    time::{Duration, Instant},
};

use bb8_redis::{
    bb8::Pool,
    redis::{cmd, Cmd, ErrorKind, FromRedisValue, RedisError, RedisResult, Value},
    RedisConnectionManager,
};
use futures::{
    future::{FusedFuture, OptionFuture},
    FutureExt,
};
use tokio::{sync::OwnedSemaphorePermit, time::sleep};
use tokio_util::sync::CancellationToken;

use crate::relay::switch::{SendError, CON_PER_WORKER, RETRY_DELAY};

use super::{
    queue::Queue, ConnectionSender, MessageID, SessionID, Switch, MESSAGE_TX, MESSAGE_TX_BYTES,
    READ_COUNT,
};

// block on read max of 5 seconds
const READ_BLOCK_MS: usize = 5000;

// connection can wait in back pressure status for 60 seconds
const MAX_BACK_PRESSURE_DURATION: Duration = Duration::from_secs(60);
const BACK_PRESSURE_RETRY: Duration = Duration::from_millis(READ_BLOCK_MS as u64);

pub struct WorkerJob<H: ConnectionSender> {
    session_id: SessionID,
    permit: OwnedSemaphorePermit,
    callback: H,
    cancellation: CancellationToken,
}

impl<H: ConnectionSender> WorkerJob<H> {
    pub fn new(
        session_id: SessionID,
        permit: OwnedSemaphorePermit,
        callback: H,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            session_id,
            permit,
            callback,
            cancellation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionStatus {
    Open,
    BackPressure { since: Instant },
    Closed,
}

impl SessionStatus {
    fn merge(&mut self, status: SessionStatus) {
        match (&self, status) {
            (Self::Closed, _) => {
                //there is no coming back from closed
            }
            (_, Self::Open) => *self = Self::Open,
            (_, Self::Closed) => *self = Self::Closed,
            (Self::Open, Self::BackPressure { since }) => *self = Self::BackPressure { since },
            (Self::BackPressure { .. }, Self::BackPressure { .. }) => {
                // we don't override the since if we still under back pressure
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Closed)
    }
}

#[derive(derive_more::Deref, derive_more::DerefMut)]
struct Session<S>
where
    S: ConnectionSender,
{
    #[deref]
    #[deref_mut]
    sender: S,
    _permit: OwnedSemaphorePermit,
    cancellation: CancellationToken,
    last_message_id: MessageID,
    status: SessionStatus,
}

impl<S> Session<S>
where
    S: ConnectionSender,
{
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
            || self.status.is_closed()
            || matches!(self.status, SessionStatus::BackPressure { since } if Instant::now().duration_since(since) > MAX_BACK_PRESSURE_DURATION)
    }

    #[inline]
    fn can_send(&self) -> bool {
        self.sender.can_send()
    }
}

impl<S> Drop for Session<S>
where
    S: ConnectionSender,
{
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl<H> From<WorkerJob<H>> for Session<H>
where
    H: ConnectionSender,
{
    fn from(value: WorkerJob<H>) -> Self {
        Self {
            sender: value.callback,
            _permit: value.permit,
            cancellation: value.cancellation,
            last_message_id: MessageID::default(),
            status: SessionStatus::Open,
        }
    }
}

pub struct Worker<H: ConnectionSender> {
    pool: Pool<RedisConnectionManager>,
    worker_id: u32,
    worker_size: usize,
    queue: Queue<WorkerJob<H>>,
    sessions: HashMap<SessionID, Session<H>>,
}

impl<H> Worker<H>
where
    H: ConnectionSender,
{
    pub fn new(worker_id: u32, worker_size: usize, switch: &Switch<H>) -> Self {
        Self {
            worker_id,
            worker_size,
            queue: switch.queue.clone(),
            pool: switch.pool.clone(),
            sessions: HashMap::default(),
        }
    }

    pub async fn start(mut self) {
        log::trace!("[{}] worker started", self.worker_id);
        // a worker will wait for available registrations.
        // once registrations are available, it will then maintain it's own list
        // of user ids (in memory) with a
        // wait for the first set of users
        let worker_name = self.worker_id.to_string();

        CON_PER_WORKER.with_label_values(&[&worker_name]).set(0);

        'main: loop {
            let mut con = match self.pool.get().await {
                Ok(con) => con,
                Err(err) => {
                    log::error!(
                        "[{}] error while getting redis connection: {}",
                        self.worker_id,
                        err
                    );
                    // this delay in case of redis connection error that we
                    // wait before retrying
                    sleep(RETRY_DELAY).await;
                    continue;
                }
            };

            // drop any connection that was cancelled or was in back pressure state for too long
            self.sessions.retain(|_, s| !s.is_cancelled());

            CON_PER_WORKER
                .with_label_values(&[&worker_name])
                .set(self.sessions.len() as i64);

            let query: OptionFuture<_> = self
                .query_command()
                .map(|cmd| {
                    async move { cmd.query_async::<_, Output>(con.deref_mut()).await }.fuse()
                })
                .into();
            let mut query = std::pin::pin!(query);

            // NOTE:
            // it's possible that the query will be None while self.sessions is not empty
            // in case if all clients are under back pressure.
            // to avoid being stuck waiting for new "jobs" forever and not be able to server
            // the back pressured client again, we need to also run a timeout
            loop {
                tokio::select! {
                    // SAFETY: query_async is not cancellation safe hence
                    // we have to run this to completion before we create a new query_async future
                    Some(output) = &mut query => {
                        // handle received
                        match output {
                            Err(err) => {
                                log::error!(
                                    "[{}] error while reading messages: {}",
                                    self.worker_id,
                                    err
                                );
                                sleep(RETRY_DELAY).await;
                            }
                            Ok(None) =>{}
                            Ok(Some(streams)) => {
                                'sender: for Messages(session_id, messages) in streams {
                                    let session = self.sessions.get_mut(&session_id).expect("must exists");

                                    // now we know that the "connected" user is indeed served by this worker
                                    // hence we can now feed the messages.
                                    for Message(msg_id, mut tags) in messages {
                                        // tags are always of even length (k, v) but we only right now
                                        // only use tag _ and value is actual content
                                        if tags.len() != 2 {
                                            log::warn!("received a message with tag count={}", tags.len());
                                            continue;
                                        }

                                        let msg = tags.swap_remove(1);
                                        let msg_len = msg.len();

                                        if let Err(err) = session.send(msg_id, msg) {
                                            match err {
                                                SendError::Closed => {
                                                    session.status.merge(SessionStatus::Closed);
                                                }
                                                SendError::NotEnoughCapacity => {
                                                    session.status.merge(SessionStatus::BackPressure { since: Instant::now() });
                                                }
                                            }
                                            continue 'sender;
                                        }
                                        // we can send, set status to Open if possible.
                                        session.status.merge(SessionStatus::Open);

                                        MESSAGE_TX.inc();
                                        MESSAGE_TX_BYTES.inc_by(msg_len as u64);

                                        session.last_message_id = msg_id;
                                    }
                                }
                            }
                        }

                        continue 'main;
                    }
                    jobs = self.queue.pop(self.worker_size.saturating_sub(self.sessions.len())) => {
                        let mut count = 0;
                        for job in jobs {
                            self.sessions.insert(job.session_id.clone(), job.into());
                            count += 1;
                        }

                        log::info!("[{}] Accepted {} new clients to handle", self.worker_id, count);

                        if query.is_terminated() {
                            // no running queries, it's safe to create a new query now
                            break;
                        }
                    }
                    _ = tokio::time::sleep(BACK_PRESSURE_RETRY), if !self.sessions.is_empty() && query.is_terminated() => {
                        // if query is terminated (no running queries) but the client has some sessions it means that
                        // all our sessions under back pressure. We have this time out to try again even if the jobs
                        // branch did not resolve.
                        // this to make sure back pressure sessions are retired again.
                        break;
                    }
                }
            }
        }
    }

    fn query_command(&self) -> Option<Cmd> {
        if self.sessions.is_empty() {
            return None;
        }

        // build command
        let (stream_ids, sessions): (Vec<&SessionID>, Vec<&Session<H>>) = self
            .sessions
            .iter()
            .filter(|(_, s)| {
                // filter out sessions that cannot send
                // messages
                s.can_send()
            })
            .unzip();

        if stream_ids.is_empty() {
            return None;
        }

        //TODO: may be not rebuild the command each time when the list of current users don't change.
        let mut cmd = cmd("XREAD");
        let mut c = cmd
            .arg("COUNT")
            .arg(READ_COUNT)
            .arg("BLOCK")
            .arg(READ_BLOCK_MS)
            .arg("STREAMS");

        for id in stream_ids {
            // convert streamID to full stream name (say stream:<id>)
            c = c.arg(id);
        }

        for session in sessions {
            c = c.arg(session.last_message_id);
        }

        Some(cmd)
    }
}

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

struct Messages(SessionID, Vec<Message>);

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
                    SessionID::from_redis_value(&values[0])?,
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
    use super::{MessageID, Output, SessionID};
    use bb8_redis::redis::{self, cmd};

    #[test]
    fn message_serialization() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut con = client.get_connection().unwrap();
        let stream = SessionID::from(0);
        let msg = "hello world";
        let _: () = cmd("XADD")
            .arg(stream.clone())
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
        assert_eq!(messages.0.to_string(), "0");
        let messages = &messages.1;

        assert_eq!(messages.len(), 1);

        let message = &messages[0];

        let tags = &message.1;
        assert_eq!(tags.len(), 2);
        let v = std::str::from_utf8(&tags[1]).unwrap();
        assert_eq!(v, "hello world");
    }
}
