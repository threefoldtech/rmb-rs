use std::str::{from_utf8, FromStr};

use crate::types::{Message, TransitMessage};

use super::{ProxyStorage, Storage};
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

enum Queue<'a> {
    Backlog(&'a str),
    Run(&'a str),
    Local,
    Forward,
    Reply,

    // for proxy
    ProxyRequest,
    ProxyReply,
}

impl std::fmt::Display for Queue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Queue::Backlog(id) => write!(f, "backlog.{}", id),
            Queue::Run(command) => write!(f, "{}", command),
            Queue::Local => write!(f, "system.local"),
            Queue::Forward => write!(f, "system.forward"),
            Queue::Reply => write!(f, "system.reply"),
            // Proxy
            Queue::ProxyRequest => write!(f, "system.proxy.request"),
            Queue::ProxyReply => write!(f, "system.proxy.reply"),
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

    #[allow(unused)]
    pub fn prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.prefix = prefix.into();
        self
    }

    #[allow(unused)]
    pub fn max_commands(mut self, max_commands: isize) -> Self {
        self.max_commands = max_commands;
        self
    }

    pub fn build(self) -> RedisStorage {
        RedisStorage::new(&self.prefix, self.pool, self.max_commands)
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

    async fn get_connection(&self) -> Result<PooledConnection<'_, RedisConnectionManager>> {
        let conn = self
            .pool
            .get()
            .await
            .context("unable to retrieve a redis connection from the pool")?;

        Ok(conn)
    }

    async fn set(
        &self,
        con: &mut PooledConnection<'_, RedisConnectionManager>,
        msg: &Message,
    ) -> Result<()> {
        // add to backlog
        let ttl = match msg.ttl() {
            Some(ttl) => ttl,
            None => bail!("message has expired"),
        };

        let key = self.prefixed(Queue::Backlog(&msg.id));
        con.set_ex(&key, msg, ttl.as_secs() as usize).await?;

        Ok(())
    }

    async fn run_with_repy(&self, queue: Queue<'_>, mut msg: Message) -> Result<()> {
        let mut conn = self.get_connection().await?;
        // set reply queue
        msg.reply = self.prefixed(queue);
        let cmd = self.prefixed(Queue::Run(&msg.command));

        conn.lpush(&cmd, msg).await?;
        conn.ltrim(&cmd, 0, self.max_commands - 1).await?;

        Ok(())
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
    async fn get(&self, id: &str) -> Result<Option<Message>> {
        let mut conn = self.get_connection().await?;
        let key = self.prefixed(Queue::Backlog(id));
        Ok(conn.get(key).await?)
    }

    async fn run(&self, msg: Message) -> Result<()> {
        self.run_with_repy(Queue::Reply, msg).await
    }

    async fn forward(&self, msg: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;

        self.set(&mut conn, msg).await?;

        // push to forward for every destination
        let queue = self.prefixed(Queue::Forward);
        for destination in &msg.destination {
            let forwarded = ForwardedMessage {
                id: msg.id.clone(),
                destination: *destination,
            };
            conn.lpush(&queue, &forwarded).await?
        }

        Ok(())
    }

    async fn reply(&self, msg: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;

        conn.lpush(&msg.reply, msg).await?;
        Ok(())
    }

    async fn local(&self) -> Result<Message> {
        let mut conn = self.get_connection().await?;
        let queue = self.prefixed(Queue::Local);
        let ret: (Vec<u8>, Message) = conn.brpop(&queue, 0).await?;

        Ok(ret.1)
    }

    async fn queued(&self) -> Result<TransitMessage> {
        let mut conn = self.get_connection().await?;
        let forward_queue = self.prefixed(Queue::Forward);
        let reply_queue = self.prefixed(Queue::Reply);
        let queues = (forward_queue.as_str(), reply_queue.as_str());

        loop {
            let ret: (String, Value) = conn.brpop(&queues, 0).await?;
            let (queue, value) = ret;

            if queue == forward_queue {
                let forward = match ForwardedMessage::from_redis_value(&value) {
                    Ok(msg) => msg,
                    Err(err) => {
                        log::debug!("cannot get forwarded message: {}", err.to_string());
                        continue;
                    }
                };

                if let Some(mut msg) = self.get(&forward.id).await? {
                    msg.destination = vec![forward.destination];
                    return Ok(TransitMessage::Request(msg));
                }
            } else if queue == reply_queue {
                // reply queue had the message itself
                // decode it directly
                let msg = Message::from_redis_value(&value)?;
                return Ok(TransitMessage::Reply(msg));
            }
        }
    }
}

#[async_trait]
impl ProxyStorage for RedisStorage {
    async fn run_proxied(&self, msg: Message) -> Result<()> {
        self.run_with_repy(Queue::ProxyReply, msg).await
    }

    async fn proxied(&self) -> Result<TransitMessage> {
        let mut conn = self.get_connection().await?;
        let request = self.prefixed(Queue::ProxyRequest);
        let response = self.prefixed(Queue::ProxyReply);
        let queues = (request.as_str(), response.as_str());

        let ret: (String, Message) = conn.brpop(&queues, 0).await?;
        let (queue, message) = ret;

        if queue == request {
            return Ok(TransitMessage::Request(message));
        } else if queue == response {
            return Ok(TransitMessage::Reply(message));
        }

        unreachable!();
    }

    async fn response(&self, msg: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;

        conn.lpush(self.prefixed(Queue::Reply), msg).await?;
        Ok(())
    }
}

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
        let storage = RedisStorage::builder(pool)
            .prefix(PREFIX)
            .max_commands(500)
            .build();

        storage
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
            data: String::from(""),
            source: 1,
            destination: vec![4],
            reply: String::from("de31075e-9af4-4933-b107-c36887d0c0f0"),
            retry: 2,
            schema: String::from(""),
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            error: None,
            signature: None,
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
        }

        let _ = storage.reply(&msg).await;
    }
}
