use super::{Identity, Signer, SIGNATURE_LENGTH};
use crate::types::Message;
use anyhow::Result;
use sp_core::{
    crypto::AccountId32,
    sr25519::{Pair as SrPair, Public},
    Pair,
};

use std::convert::From;

pub const PREFIX: u8 = 0x73; // ascii s for sr

#[derive(Clone)]
pub struct Sr25519Identity(Public);

impl Identity for Sr25519Identity {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> bool {
        verify(&self.0, sig, message)
    }
}

impl From<&AccountId32> for Sr25519Identity {
    fn from(a: &AccountId32) -> Self {
        let mut key: [u8; 32] = [0; 32];
        key.clone_from_slice(a.as_ref());
        Sr25519Identity(Public::from_raw(key))
    }
}

#[derive(Clone)]
pub struct Sr25519Signer {
    pair: SrPair,
    #[allow(unused)]
    seed: [u8; 32],
}

impl Signer for Sr25519Signer {
    fn sign<S: AsRef<[u8]>>(&self, msg: S) -> [u8; SIGNATURE_LENGTH] {
        let mut sig: [u8; SIGNATURE_LENGTH] = [0; SIGNATURE_LENGTH];
        sig[0] = PREFIX;
        sig[1..].clone_from_slice(&self.pair.sign(msg.as_ref()).0);

        sig
    }
}

impl Identity for Sr25519Signer {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> bool {
        verify(&self.pair.public(), sig, message)
    }
}

impl TryFrom<&str> for Sr25519Signer {
    type Error = anyhow::Error;

    fn try_from(phrase: &str) -> std::result::Result<Self, Self::Error> {
        // let (pair, seed) = sp_keyring::sr25519::Pair::from_phrase(phrase, None)?;
        let (pair, seed) = sp_core::sr25519::Pair::from_phrase(phrase, None)
            .map_err(|err| anyhow::anyhow!("{:?}", err))?;

        Ok(Self { pair, seed })
    }
}

fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(pk: &Public, sig: P, message: M) -> bool {
    let sig = sig.as_ref();
    assert_eq!(sig.len(), SIGNATURE_LENGTH, "invalid signature length");
    assert_eq!(sig[0], PREFIX, "invalid signature type");
    SrPair::verify_weak(&sig[1..], message, pk)
}
