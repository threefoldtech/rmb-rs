mod redis_storage;
pub use redis_storage::*;

use crate::types::{Message, QueuedMessage};
use anyhow::Result;
use async_trait::async_trait;

// operation against backlog
#[async_trait]
pub trait Storage: Clone + Send + Sync {
    // sets backlog.$uid and set ttl to $exp
    async fn set(msg: Message) -> Result<()>;

    // gets message with ID.
    async fn get(id: String) -> Result<Option<Message>>;

    // pushes the message to local process (msgbus.$cmd) queue
    async fn run(msg: Message) -> Result<()>;

    // pushes message to `msgbus.system.forward` queue
    async fn forward(msg: Message) -> Result<()>;

    // pushes message to `msg.$ret` queue
    async fn reply(msg: Message) -> Result<()>;

    // gets a message from local queue waits
    // until a message is available
    async fn local() -> Result<Message>;

    // find a better name
    // process will wait on both msgbus.system.forward AND msgbus.system.reply
    // and return the first message available with the correct Queue type
    async fn process() -> Result<QueuedMessage>;
}
