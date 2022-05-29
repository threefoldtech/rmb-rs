use std::{str::from_utf8, sync::Arc, time::Duration};

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
use lru::{DefaultHasher, LruCache};
use tokio::sync::Mutex;

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
    ttl: u64,
    max_commands: isize,
    lru_cache: Arc<Mutex<LruCache<String, Message>>>,
}

pub struct RedisStorageBuilder {
    pool: Pool<RedisConnectionManager>,
    prefix: String,
    ttl: Duration,
    cache_size: usize,
    max_commands: isize,
}

impl RedisStorageBuilder {
    pub fn new(pool: Pool<RedisConnectionManager>) -> RedisStorageBuilder {
        RedisStorageBuilder {
            pool: pool,
            prefix: String::from("msgbus"),
            ttl: Duration::from_secs(20),
            max_commands: 500,
            cache_size: 500,
        }
    }

    pub fn prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn max_commands(mut self, max_commands: isize) -> Self {
        self.max_commands = max_commands;
        self
    }

    pub fn cache_size(mut self, cache_size: usize) -> Self {
        self.cache_size = cache_size;
        self
    }

    pub fn build(&self) -> RedisStorage {
        RedisStorage::new(
            &self.prefix,
            self.pool.clone(),
            self.ttl,
            self.max_commands,
            self.cache_size,
        )
    }
}

impl RedisStorage {
    pub fn new<S: Into<String>>(
        prefix: S,
        pool: Pool<RedisConnectionManager>,
        ttl: Duration,
        max_commands: isize,
        cache_size: usize,
    ) -> Self {
        let cache = Arc::new(Mutex::new(LruCache::new(cache_size)));

        Self {
            prefix: prefix.into(),
            pool: pool,
            ttl: ttl.as_secs(),
            max_commands: max_commands,
            lru_cache: cache,
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

    async fn get_cached(&self, id: &str) -> Option<Message> {
        self.lru_cache.lock().await.pop(id)
    }

    async fn cache(&self, id: &str, msg: &Message) -> Option<Message> {
        let mut cache = self.lru_cache.lock().await;
        cache.put(id.to_owned(), msg.clone())
    }

    pub fn builder(pool: Pool<RedisConnectionManager>) -> RedisStorageBuilder {
        RedisStorageBuilder::new(pool)
    }
}

struct ForwardedMessage {
    pub id: String,
    pub destination: u32,
}

impl ForwardedMessage {
    pub fn from_str_pair<S: Into<String>>(pair: S) -> Result<Self> {
        let str_pair = pair.into();
        let parts: Vec<&str> = str_pair.rsplitn(2, '.').collect();
        let dest_str = parts[0].to_string();
        let dest = dest_str.parse::<u32>()?;

        Ok(Self {
            id: parts[1].to_string(),
            destination: dest,
        })
    }

    pub fn from_bytes_pair(bytes: &Vec<u8>) -> Result<Self> {
        let pair = from_utf8(bytes)?;
        Self::from_str_pair(pair)
    }

    pub fn to_str_pair(&self) -> String {
        format!("{}.{}", self.id, self.destination)
    }
}

impl ToRedisArgs for ForwardedMessage {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let pair = self.to_str_pair();
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
                    ErrorKind::ResponseError,
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
        let ret: Option<Vec<u8>> = conn.get(key).await?;

        match ret {
            Some(val) => {
                let msg = Message::from_json(&val)?;
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    async fn run(&self, msg: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let queue = self.prefixed(Queue::Run(&msg.command));

        conn.rpush(&queue, msg).await?;
        conn.ltrim(&queue, 0, self.max_commands).await?;

        Ok(())
    }

    async fn forward(&self, msg: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // add to backlog
        let key = self.prefixed(Queue::Backlog(&msg.id));
        conn.set_ex(&key, msg, self.ttl as usize).await?;

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
        let ret: (Vec<u8>, Vec<u8>) = conn.blpop(&queue, 0).await?;

        let (_, value) = ret;
        let msg = Message::from_json(&value)?;
        Ok(msg)
    }

    async fn queued(&self) -> Result<QueuedMessage> {
        let mut conn = self.get_connection().await?;
        let forward_queue = self.prefixed(Queue::Forward);
        let reply_queue = self.prefixed(Queue::Reply);
        let queues = (forward_queue, reply_queue);

        let result;

        loop {
            let ret: (String, Vec<u8>) = conn.blpop(&queues, 0).await?;
            let (queue, value) = ret;

            match queue {
                forward_queue => {
                    let forwarded = ForwardedMessage::from_bytes_pair(&value)?;

                    let mut ret = self.get_cached(&forwarded.id).await;
                    let not_cached = ret.is_none();

                    if not_cached {
                        ret = self.get(&forwarded.id).await?;
                    }

                    match ret {
                        Some(mut msg) => {
                            msg.destination = vec![forwarded.destination];
                            if not_cached {
                                self.cache(&msg.id, &msg);
                            }
                            result = QueuedMessage::Forward(msg);
                            break;
                        }
                        None => {
                            log::debug!("message of {} has been expired", forwarded.id);
                        }
                    }
                }

                reply_queue => {
                    // reply queue had the message itself
                    // decode it directly
                    let msg = Message::from_json(&value)?;
                    result = QueuedMessage::Reply(msg);
                    break;
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    const PREFIX: &str = "msgbus.test";
    const TTL: Duration = Duration::from_secs(20);
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
            .ttl(TTL)
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

    #[tokio::test]
    async fn test_simple_flow() {
        let storage = create_redis_storage().await;
        let id = "e60b5d65-dcf7-4894-91b9-4e546a0c0904";

        push_msg_to_local(id, &storage).await;
        let msg = storage.local().await.unwrap();
        assert_eq!(msg.id, id);

        storage.forward(&msg).await;
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
