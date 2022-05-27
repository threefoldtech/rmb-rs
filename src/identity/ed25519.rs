use super::{Identity, Signer, SIGNATURE_LENGTH};
use anyhow::Result;
use hyper::server::accept::Accept;
use sp_core::{
    crypto::AccountId32,
    ed25519::{Pair as EdPair, Public},
    Pair,
};

use std::convert::From;

pub const PREFIX: u8 = 0x65; // ascii e for ed

#[derive(Clone)]
pub struct Ed25519Identity(Public);

impl Identity for Ed25519Identity {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> bool {
        verify(&self.0, sig, message)
    }

    fn account(&self) -> AccountId32 {
        self.0.into()
    }
}

impl From<&AccountId32> for Ed25519Identity {
    fn from(a: &AccountId32) -> Self {
        let mut key: [u8; 32] = [0; 32];
        key.clone_from_slice(a.as_ref());
        Ed25519Identity(Public::from_raw(key))
    }
}

#[derive(Clone)]
pub struct Ed25519Signer {
    pair: EdPair,
    #[allow(unused)]
    seed: [u8; 32],
}

impl Signer for Ed25519Signer {
    fn sign<S: AsRef<[u8]>>(&self, msg: S) -> [u8; SIGNATURE_LENGTH] {
        let mut sig: [u8; SIGNATURE_LENGTH] = [0; SIGNATURE_LENGTH];
        sig[0] = PREFIX;
        sig[1..].clone_from_slice(&self.pair.sign(msg.as_ref()).0);

        sig
    }
}

impl Identity for Ed25519Signer {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> bool {
        verify(&self.pair.public(), sig, message)
    }

    fn account(&self) -> AccountId32 {
        self.pair.public().into()
    }
}

impl TryFrom<&str> for Ed25519Signer {
    type Error = anyhow::Error;
    fn try_from(phrase: &str) -> std::result::Result<Self, Self::Error> {
        // let (pair, seed) = sp_keyring::ed25519::Pair::from_phrase(phrase, None)?;
        let (pair, seed) = sp_core::ed25519::Pair::from_phrase(phrase, None)
            .map_err(|err| anyhow!("{:?}", err))?;

        Ok(Self { pair, seed })
    }
}

fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(pk: &Public, sig: P, message: M) -> bool {
    let sig = sig.as_ref();
    assert_eq!(sig.len(), SIGNATURE_LENGTH, "invalid signature length");
    assert_eq!(sig[0], PREFIX, "invalid signature type");
    EdPair::verify_weak(&sig[1..], message, pk)
}
