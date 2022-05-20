use crate::types::{Message, QueuedMessage};

use super::Storage;
use async_trait::async_trait;

#[derive(Clone)]
pub struct RedisStorage;

#[async_trait]
impl Storage for RedisStorage {
    async fn get(&self, _id: String) -> anyhow::Result<Option<Message>> {
        unimplemented!()
    }

    async fn run(&self, _msg: Message) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn forward(&self, _msg: Message) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn reply(&self, _msg: Message) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn local(&self) -> anyhow::Result<Message> {
        unimplemented!()
    }

    async fn queued(&self) -> anyhow::Result<crate::types::QueuedMessage> {
        Ok(QueuedMessage::Forward(Message::default()))
    }
}
