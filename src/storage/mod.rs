mod redis_storage;
pub use redis_storage::*;

use crate::types::{Message, QueuedMessage};
use anyhow::Result;
use async_trait::async_trait;

// operation against backlog
#[async_trait]
pub trait Storage: Clone + Send + Sync + 'static {
    // gets message with ID. This will retrieve the object
    // from backlog.$id. On success, this can either be None which means
    // there is no message with that ID or the actual message.
    async fn get(&self, id: &str) -> Result<Option<Message>>;

    // pushes the message to local process (msgbus.$cmd) queue.
    // this means the message will be now available to the application
    // to process.
    //
    // KNOWN ISSUE: we should not set TTL on this queue because
    // we are not sure how long the application will take to process
    // all it's queues messages. So this is potentially dangerous. A harmful
    // twin can flood a server memory by sending many large messages to a `cmd`
    // that is not handled by any application.
    //
    // SUGGESTED FIX: instead of setting TTL on the $cmd queue we can limit the length
    // of the queue. So for example, we allow maximum of 500 message to be on this queue
    // after that we need to trim the queue to specific length after push (so drop older messages)
    async fn run(&self, msg: Message) -> Result<()>;

    // forward stores the message in backlog.$id, and for each twin id in the message
    // destination, a new tuple of (id, dst) is pushed to the forward queue.
    // it also need to set TTL on the `backlog.$id` queue. This will make sure
    // message will be auto-dropped when it times out.
    async fn forward(&self, msg: Message) -> Result<()>;

    // pushes message to `msg.$ret` queue.
    async fn reply(&self, msg: Message) -> Result<()>;

    // gets a message from local queue waits
    // until a message is available
    async fn local(&self) -> Result<Message>;

    // find a better name
    // process will wait on both msgbus.system.forward AND msgbus.system.reply
    // and return the first message available with the correct Queue type
    async fn queued(&self) -> Result<QueuedMessage>;
}
