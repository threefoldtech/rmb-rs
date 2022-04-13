mod ed25519_identity;
mod message;
mod sr25519_identity;
mod twin;

pub use message::*;
pub use twin::*;

pub use ed25519_identity::*;
pub use sr25519_identity::*;

use anyhow::Result;

pub trait Identity: Clone {
    fn id(&self) -> u64;
    fn sign(&self, msg: Message) -> Result<Message>;
}
