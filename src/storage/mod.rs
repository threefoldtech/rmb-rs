mod redis;

pub use redis::*;

use crate::types::{Backlog, Envelope, JsonRequest, JsonResponse};
use anyhow::Result;
use async_trait::async_trait;

// operation against backlog
#[async_trait]
pub trait Storage: Clone + Send + Sync + 'static {
    // track stores some information about the envelope
    // in a backlog used to track replies received related to this
    // envelope. The envelope has to be a request envelope.
    async fn track(&self, msg: &Envelope) -> Result<()>;

    // gets message with ID. This will retrieve the object
    // from backlog.$id. On success, this can either be None which means
    // there is no message with that ID or the actual message.
    async fn get(&self, uid: &str) -> Result<Option<Backlog>>;

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
    async fn run(&self, msg: Envelope) -> Result<()>;

    // pushed a json response back to the caller according to his
    // reply queue.
    async fn reply(&self, queue: &str, response: JsonResponse) -> Result<()>;

    // messages waits on either "requests" or "replies" that are available to
    // be send to remote twin.
    async fn messages(&self) -> Result<Envelope>;
}
