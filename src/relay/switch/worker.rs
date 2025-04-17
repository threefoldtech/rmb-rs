use std::collections::HashMap;

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

use crate::relay::switch::{CON_PER_WORKER, RETRY_DELAY};

use super::{
    queue::Queue, Callback, MessageID, SessionID, Switch, MESSAGE_TX, MESSAGE_TX_BYTES,
    READ_BLOCK_MS, READ_COUNT,
};

pub struct WorkerJob<H: Callback> {
    session_id: SessionID,
    permit: OwnedSemaphorePermit,
    callback: H,
    cancellation: CancellationToken,
}

impl<H: Callback> WorkerJob<H> {
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

#[derive(derive_more::Deref)]
struct Session<H>
where
    H: Callback,
{
    #[deref]
    callback: H,
    _permit: OwnedSemaphorePermit,
    cancellation: CancellationToken,
    last_message_id: MessageID,
}

impl<H> From<WorkerJob<H>> for Session<H>
where
    H: Callback,
{
    fn from(value: WorkerJob<H>) -> Self {
        Self {
            callback: value.callback,
            _permit: value.permit,
            cancellation: value.cancellation,
            last_message_id: MessageID::default(),
        }
    }
}

pub struct Worker<H: Callback> {
    pool: Pool<RedisConnectionManager>,
    worker_id: u32,
    worker_size: usize,
    queue: Queue<WorkerJob<H>>,
    sessions: HashMap<SessionID, Session<H>>,
    cleanup: Vec<SessionID>,
}

impl<H> Worker<H>
where
    H: Callback,
{
    pub fn new(worker_id: u32, worker_size: usize, switch: &Switch<H>) -> Self {
        Self {
            worker_id,
            worker_size,
            queue: switch.queue.clone(),
            pool: switch.pool.clone(),
            sessions: HashMap::default(),
            cleanup: Vec::default(),
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

            // drop any connection that was cancelled.
            self.sessions.retain(|_, s| !s.cancellation.is_cancelled());

            CON_PER_WORKER
                .with_label_values(&[&worker_name])
                .set(self.sessions.len() as i64);

            let query: OptionFuture<_> = self
                .query_command()
                .map(|cmd| async move { cmd.query_async::<_, Output>(&mut *con).await }.fuse())
                .into();
            let mut query = std::pin::pin!(query);

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

                                        if session.handle(msg_id, msg).is_err() {
                                            self.cleanup.push(session_id);
                                            continue 'sender;
                                        }

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
                }
            }
        }
    }

    fn query_command(&self) -> Option<Cmd> {
        if self.sessions.is_empty() {
            return None;
        }

        // build command
        let (stream_ids, sessions): (Vec<&SessionID>, Vec<&Session<H>>) =
            self.sessions.iter().unzip();
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
