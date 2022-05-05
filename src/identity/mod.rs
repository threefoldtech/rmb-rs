mod ed25519_identity;
mod sr25519_identity;

pub use ed25519_identity::*;
pub use sr25519_identity::*;

use crate::types::Message;
use anyhow::Result;

pub trait Identity: Clone + Send + Sync {
    fn sign(&self, msg: Message) -> Result<Message>;
}

trait Identity2 {
    fn pair();
    fn sign();
}
