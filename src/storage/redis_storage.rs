use crate::types::Message;

use super::storage_interface::Storage;
use async_trait::async_trait;
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

    async fn process() -> anyhow::Result<crate::types::QueuedMessage> {
        unimplemented!()
    }
}
