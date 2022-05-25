use std::time::Duration;

use crate::types::{Message, QueuedMessage};

use super::Storage;
use anyhow::{Context, Result};
use async_trait::async_trait;
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::AsyncCommands,
    RedisConnectionManager,
};

#[derive(Clone)]
pub struct RedisStorage {
    prefix: String,
    pool: Pool<RedisConnectionManager>,
    ttl_in_seconds: usize, // in seconds,
    max_command_messages: isize,
}

// TODO: this is nearly the same as redis cache, can we move this Redis related
//       implementations to a common redis module or something?

impl RedisStorage {
    pub fn new(
        prefix: String,
        pool: Pool<RedisConnectionManager>,
        ttl_in_seconds: usize,
        max_command_messages: isize,
    ) -> Result<Self> {
        Ok(Self {
            prefix: prefix,
            pool: pool,
            ttl_in_seconds: ttl_in_seconds,
            max_command_messages: max_command_messages,
        })
    }

    // TODO: add some methods to for common redis commands
    // instead of repeating it multiple times

    pub async fn get_connection(&self) -> Result<PooledConnection<'_, RedisConnectionManager>> {
        let conn = self
            .pool
            .get()
            .await
            .context("unable to retrieve a redis connection from the pool")?;

        Ok(conn)
    }
}

// TODO: anything that can accept String and &str we can use Into<String>?
// TODO: what about losing messages while moving them between queues?

#[async_trait]
impl Storage for RedisStorage {
    async fn get(&self, id: String) -> Result<Option<Message>> {
        let mut conn = self.get_connection().await?;
        let key: String = format!("{}.backlog.{}", &self.prefix, &id);
        let ret: Option<String> = conn.get(key).await?;

        match ret {
            Some(val) => {
                let msg: Message = serde_json::from_str(&val)?;

                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    async fn run(&self, msg: Message) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let queue: String = format!("{}.{}", &self.prefix, msg.cmd);
        let value: String = serde_json::to_string(&msg)?;

        conn.rpush(&queue, &value).await?;
        conn.ltrim(&queue, 0, self.max_command_messages).await?;

        Ok(())
    }

    async fn forward(&self, msg: Message) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let value: String = serde_json::to_string(&msg)?;

        // add to backlog
        let key: String = format!("{}.backlog.{}", &self.prefix, &msg.uid);
        conn.set_ex(&key, &value, self.ttl_in_seconds).await?;

        // push to forward for every destination
        let queue: String = format!("{}.system.forward", &self.prefix);
        for dst in msg.dst {
            let value: String = format!("{}.{}", &msg.uid, &dst.to_string());
            conn.rpush(&queue, &value).await?
        }

        Ok(())
    }

    async fn reply(&self, msg: Message) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let value: String = serde_json::to_string(&msg)?;

        conn.rpush(&msg.ret, &value).await?;
        Ok(())
    }

    async fn local(&self) -> Result<Message> {
        let mut conn = self.get_connection().await?;
        let queue: String = format!("{}.system.local", &self.prefix);
        let ret: Option<Vec<String>> = conn.blpop(&queue, 0).await?;

        let vec = ret.unwrap();
        if let [_, value] = &vec[..] {
            let msg: Message = serde_json::from_str(&value)?;
            Ok(msg)
        } else {
            // better to return an error? of what type?
            unreachable!()
        }
    }

    async fn queued(&self) -> Result<Option<QueuedMessage>> {
        let mut conn = self.get_connection().await?;
        let forward_queue: String = format!("{}.system.forward", &self.prefix);
        let reply_queue: String = format!("{}.system.reply", &self.prefix);

        let queues: Vec<String> = vec![forward_queue, reply_queue];
        let ret: Option<Vec<String>> = conn.blpop(&queues, 0).await?;

        let vec = ret.unwrap();
        if let [queue, value] = &vec[..] {
            match queue {
                forward_queue => {
                    // forward queue has only <id>.<dst> pair
                    // a better way to parse these pair?
                    let mut iter = value.splitn(2, '.');
                    let id: String = String::from(iter.next().unwrap());
                    let dst: usize = iter.next().unwrap().parse().unwrap();
                    //  message can expire
                    let ret: Option<Message> = self.get(id).await?;
                    match ret {
                        Some(mut msg) => {
                            msg.dst = vec![dst];
                            Ok(Some(QueuedMessage::Forward(msg)))
                        }
                        None => {
                            // what to do in case the message is gone!
                            Ok(None)
                        }
                    }
                }
                reply_queue => {
                    // reply queue had the message itself
                    // decode it directly
                    let msg: Message = serde_json::from_str(&value)?;
                    Ok(Some(QueuedMessage::Reply(msg)))
                }
            }
        } else {
            // better to return an error? of what type?
            // unreachable!()
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    const PREFIX: &str = "msgbus.test";
    const TTL: usize = 20;
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
        let storage = RedisStorage::new(String::from(PREFIX), pool, TTL, MAX_COMMANDS)
            .context("unable to create redis storage")
            .unwrap();

        storage
    }

    async fn push_msg_to_local(id: &str, storage: &RedisStorage) -> Result<()> {
        let mut conn = storage.get_connection().await?;
        let queue: String = format!("{}.system.local", &storage.prefix);

        let msg = Message {
            ver: 1,
            uid: String::from(id),
            cmd: String::from("test.get"),
            exp: 0,
            dat: String::from(""),
            src: 0,
            dst: vec![4],
            ret: String::from("de31075e-9af4-4933-b107-c36887d0c0f0"),
            retry: 2,
            shm: String::from(""),
            now: 1653454930,
            err: String::from(""),
            sig: String::from(""),
        };

        let value: String = serde_json::to_string(&msg)?;
        conn.rpush(&queue, &value).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_all_flow() {
        let storage = create_redis_storage().await;
        let id: &str = "e60b5d65-dcf7-4894-91b9-4e546a0c0904";

        push_msg_to_local(id, &storage).await;
        let msg: Message = storage.local().await.unwrap();
        assert_eq!(msg.uid, id);

        storage.forward(msg);
        let ret: Option<QueuedMessage> = storage.queued().await.unwrap();

        match ret {
            None => println!("message is expired"), // message is expired
            Some(queued_msg) => match queued_msg {
                QueuedMessage::Forward(msg) => {
                    storage.run(msg);
                }
                QueuedMessage::Reply(msg) => {
                    storage.run(msg);
                }
            },
        }
    }
}
