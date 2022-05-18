use crate::types::{Message, QueuedMessage};

use super::Storage;
use async_trait::async_trait;

#[derive(Clone)]
pub struct RedisStorage;

#[async_trait]
impl Storage for RedisStorage {
    async fn set(_msg: Message) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn get(_id: String) -> anyhow::Result<Option<Message>> {
        unimplemented!()
    }

    async fn run(_msg: Message) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn forward(_msg: Message) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn reply(_msg: Message) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn local() -> anyhow::Result<Message> {
        unimplemented!()
    }

    async fn process(&self) -> anyhow::Result<crate::types::QueuedMessage> {
        Ok(QueuedMessage::Forward(Message::default()))
    }
}
