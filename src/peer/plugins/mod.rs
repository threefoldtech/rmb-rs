use crate::types::{Backlog, Envelope};

use super::postman::Bag;
use super::storage::JsonOutgoingRequest;
use tokio::sync::mpsc::Sender;

mod rmb;
mod upload;

pub use rmb::Rmb;
pub use upload::Upload;

#[async_trait::async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// return the unique name of the plugin. Any request that has a command that
    /// is prefixed as `$name.` will be always handed over to the module via either
    /// the `local()` method for requests that are initiated locally, or via the `remote()`
    /// methods for requests that are received from a remote peer.
    fn name(&self) -> &str;
    /// local is called to handle the outgoing requests from the local side
    /// if return None, the request then won't pushed to the remote peer and then
    /// it's up the module to reschedule the sending of the request via the sender
    ///
    /// the sender is provided on start by calling the start method.
    ///
    /// The idea is that the Module can then rewrite the outgoing request(s) or
    /// just ask the system to forward the request as is.
    async fn local(&self, request: JsonOutgoingRequest) -> Option<JsonOutgoingRequest> {
        Some(request)
    }

    /// handling incoming message. the incoming message can either be a request or a response
    /// and it's up to the module to decide what to do with it.
    ///
    /// a module can do request->response can then for example answer a coming request by pushing
    /// a response on the sender channel
    async fn remote(&self, track: Option<Backlog>, incoming: &Envelope);
    /// start this module by provided a sender channel. the channel can be then used to provide
    /// message to the system to be sent to remote peers.
    ///
    /// A module can decide then to spawn it's process that generate messages, or a simple module
    /// can just store the sender queue on its own structure and then use it to send message on a response
    /// to a delivered message via either local or remote methods
    fn start(&mut self, sender: Sender<Bag>);
}
