mod ed25519;
mod sr25519;

pub use ed25519::*;
pub use sr25519::*;

use crate::types::Message;
use anyhow::Result;

pub trait Identity: Clone + Send + Sync {
    fn verify<S: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: S, message: M) -> bool;
    fn get_public_key(&self) -> String;
}

pub trait Signer: Identity {
    fn sign<M: AsRef<[u8]>>(&self, msg: M) -> [u8; 64];
}

// Implement identity for AccountId32
impl Identity for sp_core::crypto::AccountId32 {
    fn verify<S: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: S, message: M) -> bool {
        unimplemented!();
    }

    fn get_public_key(&self) -> String {
        unimplemented!()
    }
}
