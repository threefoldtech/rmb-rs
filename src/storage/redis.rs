use std::{
    fmt::Display,
    str::{from_utf8, FromStr},
};

use crate::types::{Envelope, EnvelopeExt, JsonRequest, JsonResponse};

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

struct BacklogKey<'a>(&'a str);

impl<'a> ToRedisArgs for BacklogKey<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + bb8_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self)
    }
}

impl<'a> Display for BacklogKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "msgbus.backlog.{}", self.0)
    }
}

struct RunKey<'a>(&'a str);

impl<'a> ToRedisArgs for RunKey<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + bb8_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self)
    }
}

impl<'a> Display for RunKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "msgbus.backlog.{}", self.0)
    }
}

enum Queue {
    Local,
    Forward,
    Reply,
}

impl std::fmt::Display for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Queue::Local => write!(f, "msgbus.system.local"),
            Queue::Forward => write!(f, "msgbus.system.forward"),
            Queue::Reply => write!(f, "msgbus.system.reply"),
        }
    }
}

impl ToRedisArgs for Queue {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + bb8_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self)
    }
}

#[derive(Clone)]
pub struct RedisStorage {
    pool: Pool<RedisConnectionManager>,
    max_commands: isize,
}

pub struct RedisStorageBuilder {
    pool: Pool<RedisConnectionManager>,
    max_commands: isize,
}

impl RedisStorageBuilder {
    pub fn new(pool: Pool<RedisConnectionManager>) -> RedisStorageBuilder {
        RedisStorageBuilder {
            pool,
            max_commands: 500,
        }
    }

    #[allow(unused)]
    pub fn max_commands(mut self, max_commands: isize) -> Self {
        self.max_commands = max_commands;
        self
    }

    pub fn build(self) -> RedisStorage {
        RedisStorage::new(self.pool, self.max_commands)
    }
}

impl RedisStorage {
    pub fn new(pool: Pool<RedisConnectionManager>, max_commands: isize) -> Self {
        Self { pool, max_commands }
    }

    async fn get_connection(&self) -> Result<PooledConnection<'_, RedisConnectionManager>> {
        let conn = self
            .pool
            .get()
            .await
            .context("unable to retrieve a redis connection from the pool")?;

        Ok(conn)
    }

    async fn set(&self, env: &Envelope) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // add to backlog
        let ttl = match env.ttl() {
            Some(ttl) => ttl,
            None => bail!("message has expired"),
        };

        let key = BacklogKey(&env.uid);
        conn.set_ex(&key, env, ttl.as_secs() as usize)
            .await
            .with_context(|| format!("failed to set message ttl to '{}'", ttl.as_secs()))?;

        Ok(())
    }

    async fn get_from<K, O: FromRedisValue>(&self, key: K) -> Result<Option<O>>
    where
        K: ToRedisArgs + Send + Sync,
    {
        let mut conn = self.get_connection().await?;
        Ok(conn.get(key).await?)
    }

    // fn prefixed<T: Display>(&self, o: T) -> String {
    //     format!("{}.{}", self.prefix, o)
    // }

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
        if parts.len() != 2 {
            bail!("invalid message address string");
        }

        let out: u32 = parts[0]
            .parse()
            .with_context(|| format!("invalid destination twin format {}", parts[0]))?;

        Ok(Self {
            id: parts[1].into(),
            destination: out,
        })
    }
}

impl std::fmt::Display for ForwardedMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.id, self.destination)
    }
}

impl ForwardedMessage {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let pair = from_utf8(bytes)?;
        pair.parse()
    }
}

impl ToRedisArgs for ForwardedMessage {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let pair = self.to_string();
        out.write_arg(pair.as_bytes());
    }
}

impl FromRedisValue for ForwardedMessage {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        if let Value::Data(data) = v {
            ForwardedMessage::from_bytes(data).map_err(|e| {
                RedisError::from((
                    ErrorKind::TypeError,
                    "cannot decode a forwarded message from from pair {}",
                    e.to_string(),
                ))
            })
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
    async fn get(&self, id: &str) -> Result<Option<Envelope>> {
        self.get_from(BacklogKey(id)).await
    }

    async fn run(&self, env: Envelope) -> Result<()> {
        let request: JsonRequest = env.try_into().context("failed to extract context")?;
        let mut conn = self.get_connection().await?;
        // set reply queue

        let key = RunKey(&request.command);
        request.reply = Queue::Reply.to_string();

        conn.lpush(&key, request).await?;
        conn.ltrim(&key, 0, self.max_commands - 1).await?;

        Ok(())
    }

    async fn forward(&self, msg: &Envelope) -> Result<()> {
        let mut conn = self.get_connection().await?;

        self.set(msg).await?;

        // push to forward for every destination
        let queue = Queue::Forward.to_string();
        for destination in &msg.destinations {
            let forwarded = ForwardedMessage {
                id: msg.uid.clone(),
                destination: *destination,
            };
            conn.lpush(&queue, &forwarded).await?
        }

        Ok(())
    }

    async fn local(&self) -> Result<JsonRequest> {
        let mut conn = self.get_connection().await?;
        let ret: (Vec<u8>, JsonRequest) = conn.brpop(Queue::Local, 0).await?;

        Ok(ret.1)
    }

    async fn queued(&self) -> Result<Envelope> {
        let mut conn = self.get_connection().await?;
        let forward_queue = Queue::Forward.to_string();
        let reply_queue = Queue::Reply.to_string();
        let queues = (forward_queue.as_str(), reply_queue.as_str());

        loop {
            let (queue, value): (String, Value) = conn.brpop(&queues, 0).await?;

            if queue == forward_queue {
                let forward = match ForwardedMessage::from_redis_value(&value) {
                    Ok(msg) => msg,
                    Err(err) => {
                        log::debug!("cannot get forwarded message: {}", err.to_string());
                        continue;
                    }
                };

                if let Some(mut env) = self.get(&forward.id).await? {
                    env.destinations = vec![forward.destination];
                    return Ok(env);
                }
            } else if queue == reply_queue {
                // reply queue had the message itself
                // decode it directly
                let response = JsonResponse::from_redis_value(&value)?;
                let env: Envelope = match response.try_into() {
                    Ok(env) => env,
                    Err(err) => {
                        log::error!("failed to build envelope from response");
                        continue;
                    }
                };
                return Ok(env);
            }
        }
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: &str = "msgbus.test";

    async fn create_redis_storage() -> RedisStorage {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/")
            .context("unable to create redis connection manager")
            .unwrap();
        let pool = Pool::builder()
            .build(manager)
            .await
            .context("unable to build pool or redis connection manager")
            .unwrap();

        RedisStorage::builder(pool).max_commands(500).build()
    }

    async fn push_msg_to_local(id: &str, storage: &RedisStorage) -> Result<()> {
        let mut conn = storage.get_connection().await?;
        let queue = storage.prefixed(Queue::Local);

        use std::time::SystemTime;
        let msg = Message {
            version: 1,
            id: String::from(id),
            command: String::from("test.get"),
            expiration: 300,
            source: 1,
            destination: vec![4],
            reply: String::from("de31075e-9af4-4933-b107-c36887d0c0f0"),
            retry: 2,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            ..Message::default()
        };

        conn.lpush(&queue, &msg).await?;

        Ok(())
    }

    #[test]
    fn test_parse_forwarded_message() {
        let s = "a.b.c.1";
        let ret: ForwardedMessage = s.parse().expect("failed to parse 'a.b.c.1'");

        assert_eq!(ret.id, "a.b.c");
        assert_eq!(ret.destination, 1);

        let bad_s = "abc";
        let err_ret = bad_s.parse::<ForwardedMessage>();
        assert_eq!(err_ret.is_err(), true);
    }

    #[tokio::test]
    async fn test_simple_flow() {
        let storage = create_redis_storage().await;
        let id = "e60b5d65-dcf7-4894-91b9-4e546a0c0904";

        let _ = push_msg_to_local(id, &storage).await;
        let msg = storage.local().await.unwrap();
        assert_eq!(msg.id, id);

        let _ = storage.forward(&msg).await;

        let opt = storage.get(id).await.unwrap();
        assert_eq!(opt.is_some(), true);

        let queued_msg = storage.queued().await.unwrap();
        match queued_msg {
            TransitMessage::Request(msg) => {
                let _ = storage.run(msg).await;
            }
            TransitMessage::Reply(msg) => {
                let _ = storage.run(msg).await;
            }
            TransitMessage::Upload(_) => (),
        }

        let _ = storage.reply(&msg).await;
    }
}
*/
