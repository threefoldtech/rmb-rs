use std::{
    str::{from_utf8, FromStr},
    sync::Arc,
};

use crate::types::{Message, QueuedMessage};

use super::Storage;
use anyhow::{Context, Result};
use async_trait::async_trait;
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::{
        AsyncCommands, ErrorKind, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs,
        Value,
    },
    RedisConnectionManager,
};
use tokio::sync::Mutex;

// max TTL in seconds = 1 hour
const MAX_TTL: usize = 3600;

enum Queue<'a> {
    Backlog(&'a str),
    Run(&'a str),
    Local,
    Forward,
    Reply,
}

impl std::fmt::Display for Queue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Queue::Backlog(id) => write!(f, "backlog.{}", id),
            Queue::Run(command) => write!(f, "{}", command),
            Queue::Local => write!(f, "system.local"),
            Queue::Forward => write!(f, "system.forward"),
            Queue::Reply => write!(f, "system.reply"),
        }
    }
}

#[derive(Clone)]
pub struct RedisStorage {
    prefix: String,
    pool: Pool<RedisConnectionManager>,
    max_commands: isize,
}

pub struct RedisStorageBuilder {
    pool: Pool<RedisConnectionManager>,
    prefix: String,
    max_commands: isize,
}

impl RedisStorageBuilder {
    pub fn new(pool: Pool<RedisConnectionManager>) -> RedisStorageBuilder {
        RedisStorageBuilder {
            pool,
            prefix: String::from("msgbus"),
            max_commands: 500,
        }
    }

    pub fn prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn max_commands(mut self, max_commands: isize) -> Self {
        self.max_commands = max_commands;
        self
    }

    pub fn build(&self) -> RedisStorage {
        RedisStorage::new(&self.prefix, self.pool.clone(), self.max_commands)
    }
}

impl RedisStorage {
    pub fn new<S: Into<String>>(
        prefix: S,
        pool: Pool<RedisConnectionManager>,
        max_commands: isize,
    ) -> Self {
        Self {
            prefix: prefix.into(),
            pool,
            max_commands,
        }
    }

    pub async fn get_connection(&self) -> Result<PooledConnection<'_, RedisConnectionManager>> {
        let conn = self
            .pool
            .get()
            .await
            .context("unable to retrieve a redis connection from the pool")?;

        Ok(conn)
    }

    fn prefixed(&self, queue: Queue) -> String {
        format!("{}.{}", self.prefix, queue)
    }

    pub fn builder(pool: Pool<RedisConnectionManager>) -> RedisStorageBuilder {
        RedisStorageBuilder::new(pool)
    }
}

struct ForwardedMessage {
    pub id: String,
    pub destination: u32,
}

impl FromStr for ForwardedMessage {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.rsplitn(2, '.').collect();
        let dest_str = parts[0].to_string();

        let out = dest_str.parse::<u32>();

        match out {
            Ok(dest) => Ok(Self {
                id: parts[1].to_string(),
                destination: dest,
            }),
            Err(_) => Err(anyhow!(
                "cannot parse forwarded message from string of {}",
                s
            )),
        }
    }
}

impl ToString for ForwardedMessage {
    fn to_string(&self) -> String {
        format!("{}.{}", self.id, self.destination)
    }
}

impl ForwardedMessage {
    pub fn from_bytes_pair(bytes: &Vec<u8>) -> Result<Self> {
        let pair = from_utf8(bytes)?;
        pair.parse::<Self>()
    }
}

impl ToRedisArgs for ForwardedMessage {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let pair = self.to_string();
        out.write_arg(&pair.as_bytes());
    }
}

impl FromRedisValue for ForwardedMessage {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        if let Value::Data(data) = v {
            let ret = ForwardedMessage::from_bytes_pair(data);
            match ret {
                Ok(bytes) => Ok(bytes),
                Err(err) => Err(RedisError::from((
                    ErrorKind::TypeError,
                    "cannot decode a forwarded message from from pair {}",
                    err.to_string(),
                ))),
            }
        } else {
            Err(RedisError::from((
                ErrorKind::TypeError,
                "expected a data type from redis",
            )))
        }
    }
}

#[async_trait]
impl Storage for RedisStorage {
    async fn get(&self, id: &str) -> Result<Option<Message>> {
        let mut conn = self.get_connection().await?;
        let key = self.prefixed(Queue::Backlog(id));
        Ok(conn.get(key).await?)
    }

    async fn run(&self, msg: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let queue = self.prefixed(Queue::Run(&msg.command));

        conn.rpush(&queue, msg).await?;

        // only keep `max_commands` in this queue
        let len: isize = conn.llen(&queue).await?;
        if len > self.max_commands {
            conn.ltrim(&queue, self.max_commands, -1).await?;
        }

        Ok(())
    }

    async fn forward(&self, msg: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // add to backlog
        let key = self.prefixed(Queue::Backlog(&msg.id));
        let mut expiration = msg.expiration;
        if expiration == 0 {
            expiration = MAX_TTL;
        }
        conn.set_ex(&key, msg, expiration).await?;

        // push to forward for every destination
        let queue = self.prefixed(Queue::Forward);
        for destination in &msg.destination {
            let forwarded = ForwardedMessage {
                id: msg.id.to_owned(),
                destination: *destination,
            };
            conn.rpush(&queue, &forwarded).await?
        }

        Ok(())
    }

    async fn reply(&self, msg: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;

        conn.rpush(&msg.reply, msg).await?;
        Ok(())
    }

    async fn local(&self) -> Result<Message> {
        let mut conn = self.get_connection().await?;
        let queue = self.prefixed(Queue::Local);
        let ret: (Vec<u8>, Message) = conn.blpop(&queue, 0).await?;

        Ok(ret.1)
    }

    async fn queued(&self) -> Result<QueuedMessage> {
        let mut conn = self.get_connection().await?;
        let forward_queue = self.prefixed(Queue::Forward);
        let reply_queue = self.prefixed(Queue::Reply);
        let queues = (forward_queue, reply_queue);

        loop {
            let ret: (String, Value) = conn.blpop(&queues, 0).await?;
            let (queue, value) = ret;

            match queue {
                forward_queue => {
                    let forwarded;

                    let ret = ForwardedMessage::from_redis_value(&value);
                    match ret {
                        Ok(msg) => forwarded = msg,
                        Err(err) => {
                            log::debug!("cannot get forwarded message: {}", err.to_string());
                            continue;
                        }
                    }

                    let ret = self.get(&forwarded.id).await?;
                    match ret {
                        Some(mut msg) => {
                            msg.destination = vec![forwarded.destination];
                            return Ok(QueuedMessage::Forward(msg));
                        }
                        None => {
                            log::debug!("message of {} has been expired", forwarded.id);
                        }
                    }
                }

                reply_queue => {
                    // reply queue had the message itself
                    // decode it directly
                    let msg = Message::from_redis_value(&value)?;
                    return Ok(QueuedMessage::Reply(msg));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow;
    use serde::Deserialize;

    use super::*;

    const PREFIX: &str = "msgbus.test";
    const MAX_COMMANDS: isize = 500;

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

    async fn push_msg_to_local(id: &str, storage: &RedisStorage) -> Result<()> {
        let mut conn = storage.get_connection().await?;
        let queue = storage.prefixed(Queue::Local);

        let msg = Message {
            version: 1,
            id: String::from(id),
            command: String::from("test.get"),
            expiration: 0,
            data: String::from(""),
            source: 1,
            destination: vec![4],
            reply: String::from("de31075e-9af4-4933-b107-c36887d0c0f0"),
            retry: 2,
            schema: String::from(""),
            now: 1653454930,
            error: None,
            signature: String::from(""),
        };

        conn.rpush(&queue, &msg).await?;

        Ok(())
    }

    #[test]
    fn test_parse_forwarded_message() {
        let s = "a.b.c.1";
        let ret = s.parse::<ForwardedMessage>();
        let msg = ret.unwrap();

        assert_eq!(msg.id, "a.b.c");
        assert_eq!(msg.destination, 1);

        let bad_s = "abc";
        let err_ret = bad_s.parse::<ForwardedMessage>();
        assert_eq!(err_ret.is_err(), true);
    }

    #[tokio::test]
    async fn test_simple_flow() {
        let storage = create_redis_storage().await;
        let id = "e60b5d65-dcf7-4894-91b9-4e546a0c0904";

        push_msg_to_local(id, &storage).await;
        let msg = storage.local().await.unwrap();
        assert_eq!(msg.id, id);

        storage.forward(&msg).await;

        let opt = storage.get(id).await.unwrap();
        assert_eq!(opt.is_some(), true);

        let queued_msg = storage.queued().await.unwrap();
        match queued_msg {
            QueuedMessage::Forward(msg) => {
                storage.run(&msg).await;
            }
            QueuedMessage::Reply(msg) => {
                storage.run(&msg).await;
            }
        }

        storage.reply(&msg).await;
    }
}
