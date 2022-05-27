mod ed25519;
mod sr25519;

pub use ed25519::{Ed25519Identity, Ed25519Signer, PREFIX as ED_PREFIX};
pub use sr25519::{Sr25519Identity, Sr25519Signer, PREFIX as SR_PREFIX};

use sp_core::crypto::AccountId32;

pub const SIGNATURE_LENGTH: usize = 65;

pub trait Identity: Clone + Send + Sync {
    fn verify<S: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: S, message: M) -> bool;
    fn account(&self) -> AccountId32;
}

pub trait Signer: Identity {
    /// sign a message. the returned signature is a 64 bytes signature prefixed with
    /// one byte which indicates the type of the key.
    fn sign<M: AsRef<[u8]>>(&self, message: M) -> [u8; SIGNATURE_LENGTH];
}

// Implement identity for AccountId32
impl Identity for AccountId32 {
    fn verify<S: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: S, message: M) -> bool {
        // this is a tricky one because we don't know what kind of `key` the <account id 32> is
        // so we have to know that from the signature itself. We will assume that signers will always
        // prefix the signature with their type.
        // so instead of the standard 64 bytes signatures, we will have a signature of 65 bytes length
        let sig = sig.as_ref();
        assert_eq!(sig.len(), SIGNATURE_LENGTH, "invalid signature length");
        match sig[0] {
            ED_PREFIX => {
                let pk = Ed25519Identity::from(self);
                pk.verify(sig, message)
            }
            SR_PREFIX => {
                let pk = Sr25519Identity::from(self);
                pk.verify(sig, message)
            }
            _ => false,
        }
    }

    fn account(&self) -> AccountId32 {
        self.clone()
    }
}

#[derive(Clone)]
pub enum Signers {
    Ed25519(Ed25519Signer),
    Sr25519(Sr25519Signer),
}

impl Identity for Signers {
    fn verify<S: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: S, message: M) -> bool {
        match self {
            Signers::Ed25519(ref sk) => sk.verify(sig, message),
            Signers::Sr25519(ref sk) => sk.verify(sig, message),
        }
    }

    fn account(&self) -> AccountId32 {
        match self {
            Signers::Ed25519(ref sk) => sk.account(),
            Signers::Sr25519(ref sk) => sk.account(),
        }
    }
}

impl Signer for Signers {
    fn sign<M: AsRef<[u8]>>(&self, message: M) -> [u8; SIGNATURE_LENGTH] {
        match self {
            Signers::Ed25519(ref sk) => sk.sign(message),
            Signers::Sr25519(ref sk) => sk.sign(message),
        }
    }
}
