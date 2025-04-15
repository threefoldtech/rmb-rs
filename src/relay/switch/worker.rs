use std::collections::HashMap;

use bb8_redis::{
    bb8::Pool,
    redis::{cmd, ErrorKind, FromRedisValue, RedisError, RedisResult, Value},
    RedisConnectionManager,
};
use tokio::time::{sleep, Instant, Duration};
use tokio_util::sync::CancellationToken;

use crate::relay::switch::{SwitchError, CallbackError, CON_PER_WORKER, MIN_JOBS_POP, RETRY_DELAY};

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
    // cooldown information
    cooldown_until: Option<Instant>,
    first_error_time: Option<Instant>,
    consecutive_errors: u32,
}

impl<H: Callback> Session<H> {
    fn new(callback: H, cancellation: CancellationToken) -> Self {
        Self {
            callback,
            cancellation,
            last_message_id: MessageID::default(),
            cooldown_until: None,
            first_error_time: None,
            consecutive_errors: 0,
        }
    }
    
    fn is_in_cooldown(&self, now: Instant) -> bool {
        if let Some(until) = self.cooldown_until {
            now < until
        } else {
            false
        }
    }
    
    fn reset_cooldown(&mut self) {
        self.cooldown_until = None;
        self.first_error_time = None;
        self.consecutive_errors = 0;
    }
    
    fn apply_cooldown(&mut self, now: Instant, base_duration: Duration, max_duration: Duration, multiplier: f32) {
        // while processing a batch of messages, we want to avoid applying multiple cooldowns.
        // Applying them repeatedly wouldn't be effective until the entire batch is processed.
        // So we check if the cooldown is already active to avoid unnecessary updates.
         if self.cooldown_until.is_none() || now >= self.cooldown_until.unwrap() {
            self.consecutive_errors += 1;
            
            if self.first_error_time.is_none() {
                self.first_error_time = Some(now);
            }
            
            // Calculate cooldown duration
            let duration = self.calculate_cooldown_duration(now, base_duration, max_duration, multiplier);
            
            // Apply cooldown
            self.cooldown_until = Some(now + duration);
        }
    }
    
    fn calculate_cooldown_duration(&self, now: Instant, base_duration: Duration, max_duration: Duration, multiplier: f32) -> Duration {
        // If this is the first error, use base duration
        if self.consecutive_errors == 1 {
            return base_duration;
        }
        
        // Calculate how long errors have been occurring
        let error_duration = if let Some(first_time) = self.first_error_time {
            now.duration_since(first_time)
        } else {
            Duration::from_secs(0)
        };
        
        // For recent first errors (< 6 seconds), use base duration
        if error_duration < Duration::from_secs(6) {
            return base_duration;
        }
        
        // For persistent errors, apply exponential backoff
        let factor = (self.consecutive_errors as f32 - 1.0).min(10.0);
        let duration = base_duration.mul_f32(multiplier.powf(factor));
        
        // Cap at maximum duration
        std::cmp::min(duration, max_duration)
    }
}

pub struct Worker<H: Callback> {
    pool: Pool<RedisConnectionManager>,
    worker_id: u32,
    worker_size: usize,
    queue: Queue<WorkerJob<H>>,
    sessions: HashMap<SessionID, Session<H>>,
    cleanup: Vec<SessionID>,

    // Cooldown configuration
    cooldown_base_duration: Duration,
    cooldown_max_duration: Duration,
    cooldown_multiplier: f32,
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
            cooldown_base_duration: Duration::from_millis(100),
            cooldown_max_duration: Duration::from_secs(5),
            cooldown_multiplier: 2.0,
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
                let session = Session::new(job.callback, job.cancellation);

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
                    let session = Session::new(job.callback, job.cancellation);

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

        let now = Instant::now();
        // Filter active sessions (not in cooldown)
        let active_sessions: Vec<(&SessionID, &Session<H>)> = self.sessions.iter()
            .filter(|(_, session)| !session.is_in_cooldown(now))
            .collect();

        if active_sessions.is_empty() {
            return Ok(());
        }

        // build command
        let (stream_ids, sessions): (Vec<&SessionID>, Vec<&Session<H>>) =
            active_sessions.into_iter().unzip();
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

                match session.callback.handle(msg_id, msg) {
                    Ok(()) => {
                        // Success case - update metrics and reset cooldown
                        MESSAGE_TX.inc();
                        MESSAGE_TX_BYTES.inc_by(msg_len as u64);
                        session.last_message_id = msg_id;
                        
                        if session.consecutive_errors > 0 {
                            session.reset_cooldown();
                        }
                    },
                    Err(CallbackError::Closed) => {
                        // Channel is closed - client disconnected or crashed
                        log::debug!("Channel closed for session {}, cleaning up", session_id);
                        self.cleanup.push(session_id);
                        continue 'sender;
                    },
                    Err(CallbackError::Full) => {
                        // Channel is full - client is slow
                        log::debug!("Channel full for session {}, applying cooldown", session_id);
                        
                        // Apply cooldown
                        session.apply_cooldown(
                            now,
                            self.cooldown_base_duration,
                            self.cooldown_max_duration,
                            self.cooldown_multiplier
                        );
                    
                        // Log cooldown details
                        if let Some(until) = session.cooldown_until {
                            let duration = until.duration_since(now);
                            log::debug!(
                                "Applied cooldown for session {} until {:?} (duration: {:?}, consecutive errors: {})",
                                session_id, until, duration, session.consecutive_errors
                            );
                        }
                    
                    // Skip this message and continue with others
                    continue;
                    }
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
