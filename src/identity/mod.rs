mod ed25519_identity;
mod sr25519_identity;

pub use ed25519_identity::*;
pub use sr25519_identity::*;

use crate::types::Message;
use anyhow::Result;

pub trait Identity: Clone + Send + Sync {
    fn sign(&self, msg: Message) -> Result<Message>;
    fn verify<M: AsRef<str>>(&self, sig: &[u8], msg: M, pub_key: &[u8]) -> bool;
    fn get_public_key(&self) -> String;
}

trait Identity2 {
    fn pair();
    fn sign();
}
