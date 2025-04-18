use std::fmt::Display;

use crate::types::Backlog;

use super::{
    JsonIncomingRequest, JsonIncomingResponse, JsonMessage, JsonOutgoingRequest,
    JsonOutgoingResponse, Storage,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::{AsyncCommands, FromRedisValue, ToRedisArgs, Value},
    RedisConnectionManager,
};

struct BacklogKey<'a>(&'a str);

const MAX_COMMANDS: isize = 10000;

impl ToRedisArgs for BacklogKey<'_> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + bb8_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self)
    }
}

impl Display for BacklogKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "msgbus.backlog.{}", self.0)
    }
}

struct RunKey<'a>(&'a str);

impl ToRedisArgs for RunKey<'_> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + bb8_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self)
    }
}

impl Display for RunKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "msgbus.{}", self.0)
    }
}

enum Queue {
    Request,
    Response,
}

impl AsRef<str> for Queue {
    fn as_ref(&self) -> &str {
        match self {
            Queue::Request => "msgbus.system.local",
            Queue::Response => "msgbus.system.reply",
        }
    }
}

impl std::fmt::Display for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Queue::Request => write!(f, "msgbus.system.local"),
            Queue::Response => write!(f, "msgbus.system.reply"),
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
            max_commands: MAX_COMMANDS,
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

#[async_trait]
impl Storage for RedisStorage {
    async fn track(&self, backlog: &Backlog) -> Result<()> {
        if backlog.uid.is_empty() {
            anyhow::bail!("backlog has no uid");
        }
        let mut conn = self.get_connection().await?;
        let key = BacklogKey(&backlog.uid);
        let _: () = conn.set_ex(&key, backlog, backlog.ttl as usize).await?;

        Ok(())
    }

    async fn get(&self, uid: &str) -> Result<Option<Backlog>> {
        self.get_from(BacklogKey(uid)).await
    }

    async fn request(&self, mut request: JsonIncomingRequest) -> Result<()> {
        let mut conn = self.get_connection().await?;
        // set reply queue
        request.reply_to = Queue::Response.to_string();

        let key = RunKey(&request.command);
        let _: () = conn.lpush(&key, &request).await?;
        let _: () = conn.ltrim(&key, 0, self.max_commands - 1).await?;

        Ok(())
    }

    async fn response(&self, queue: &str, response: JsonIncomingResponse) -> Result<()> {
        let mut conn = self.get_connection().await?;
        // set reply queue

        let _: () = conn.lpush(queue, &response).await?;
        let _: () = conn.ltrim(queue, 0, self.max_commands - 1).await?;

        Ok(())
    }

    async fn messages(&self) -> Result<JsonMessage> {
        let mut conn = self.get_connection().await?;
        let req_queue = Queue::Request.as_ref();
        let resp_queue = Queue::Response.as_ref();
        let queues = (req_queue, resp_queue);

        let (queue, value): (String, Value) = conn.brpop(queues, 0).await?;

        let msg: JsonMessage = if queue == req_queue {
            JsonOutgoingRequest::from_redis_value(&value)
                .context("failed to load json request")?
                .into()
        } else {
            // reply queue had the message itself
            // decode it directly
            JsonOutgoingResponse::from_redis_value(&value)
                .context("failed to load json response")?
                .into()
        };

        Ok(msg)
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
