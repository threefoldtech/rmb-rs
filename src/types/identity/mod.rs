mod ed25519_identity;
mod sr25519_identity;

pub use ed25519_identity::*;
pub use sr25519_identity::*;

use crate::types::Message;
use anyhow::Result;

pub trait Identity: Clone {
    fn id(&self) -> u64;
    fn sign(&self, msg: Message) -> Result<Message>;
}
