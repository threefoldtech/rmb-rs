use std::collections::HashMap;

use bb8_redis::{
    bb8::Pool,
    redis::{cmd, ErrorKind, FromRedisValue, RedisError, RedisResult, Value},
    RedisConnectionManager,
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::relay::switch::{SwitchError, CON_PER_WORKER, MIN_JOBS_POP, RETRY_DELAY};

use super::{
    queue::Queue, Callback, MessageID, SessionID, Switch, MESSAGE_TX, MESSAGE_TX_BYTES,
    READ_BLOCK_MS, READ_COUNT,
};

pub struct WorkerJob<H: Callback> {
    session_id: SessionID,
    callback: H,
    cancellation: CancellationToken,
}

impl<H: Callback> WorkerJob<H> {
    pub fn new(session_id: SessionID, callback: H, cancellation: CancellationToken) -> Self {
        Self {
            session_id,
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
    cancellation: CancellationToken,
    last_message_id: MessageID,
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

        loop {
            log::trace!("[{}] waiting for connections", self.worker_id);
            for job in self
                .queue
                .pop(self.worker_size.min(MIN_JOBS_POP))
                .await
                .into_iter()
            {
                let session = Session {
                    callback: job.callback,
                    cancellation: job.cancellation,
                    last_message_id: MessageID::default(),
                };

                self.sessions.insert(job.session_id, session);
            }

            'inner: loop {
                log::debug!(
                    "[{}] handling {} connections",
                    self.worker_id,
                    self.sessions.len()
                );

                CON_PER_WORKER
                    .with_label_values(&[&worker_name])
                    .set(self.sessions.len() as i64);

                // process
                match self.process().await {
                    Ok(_) => {}
                    Err(SwitchError::RedisError(err)) => {
                        log::error!(
                            "[{}] error while waiting for messages: {}",
                            self.worker_id,
                            err
                        );
                        // this delay in case of redis connection error that we
                        // wait before retrying
                        sleep(RETRY_DELAY).await;
                    }
                    Err(err) => {
                        log::error!(
                            "[{}] error while waiting for messages: {}",
                            self.worker_id,
                            err
                        );
                    }
                };

                // no more sessions to serve
                if self.sessions.is_empty() {
                    // make sure to set this back to 0
                    CON_PER_WORKER.with_label_values(&[&worker_name]).set(0);

                    break 'inner;
                }

                // can we take more users?
                if self.sessions.len() >= self.worker_size {
                    // no.
                    continue;
                }

                for job in self
                    .queue
                    .pop_no_wait(usize::min(
                        self.worker_size.saturating_sub(self.sessions.len()),
                        MIN_JOBS_POP,
                    ))
                    .await
                    .into_iter()
                {
                    let session = Session {
                        callback: job.callback,
                        cancellation: job.cancellation,
                        last_message_id: MessageID::default(),
                    };

                    self.sessions.insert(job.session_id, session);
                }
            }
        }
    }

    async fn process(&mut self) -> Result<(), SwitchError> {
        self.sessions.retain(|_, s| !s.cancellation.is_cancelled());

        if self.sessions.is_empty() {
            return Ok(());
        }

        // build command
        let (stream_ids, sessions): (Vec<&SessionID>, Vec<&Session<H>>) =
            self.sessions.iter().unzip();
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

        for session in sessions {
            c = c.arg(session.last_message_id);
        }

        // now actually run the command
        let mut con = self.pool.get().await?;

        let output: Output = c.query_async(&mut *con).await?;

        let output = match output {
            None => return Ok(()),
            Some(streams) => streams,
        };

        'sender: for Messages(session_id, messages) in output {
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

        // make sure to cancel all unreachable peers to clean up
        for session_id in self.cleanup.drain(..) {
            if let Some(session) = self.sessions.remove(&session_id) {
                session.cancellation.cancel();
            }
        }

        Ok(())
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
