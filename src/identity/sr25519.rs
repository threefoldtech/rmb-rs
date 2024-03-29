use super::{validate_signature_len, Identity, Signer, SIGNATURE_LENGTH};
use anyhow::Result;

use std::convert::From;
use subxt::ext::sp_core::{
    sr25519::{Pair as SrPair, Public},
    Pair,
};
use subxt::utils::AccountId32;
use tfchain_client::client::KeyPair;

pub const PREFIX: u8 = 0x73; // ascii s for sr

#[derive(Clone)]
pub struct Sr25519Identity(Public);

impl Identity for Sr25519Identity {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> Result<()> {
        verify(&self.0, sig, message)
    }

    fn account(&self) -> AccountId32 {
        self.0.into()
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
}

impl Signer for Sr25519Signer {
    fn sign<S: AsRef<[u8]>>(&self, msg: S) -> [u8; SIGNATURE_LENGTH] {
        let mut sig: [u8; SIGNATURE_LENGTH] = [0; SIGNATURE_LENGTH];
        sig[0] = PREFIX;
        sig[1..].clone_from_slice(&self.pair.sign(msg.as_ref()).0);

        sig
    }

    fn pair(&self) -> KeyPair {
        KeyPair::Sr25519(self.pair.clone())
    }
}

impl Identity for Sr25519Signer {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> Result<()> {
        verify(&self.pair.public(), sig, message)
    }

    fn account(&self) -> AccountId32 {
        self.pair.public().into()
    }
}

impl TryFrom<&str> for Sr25519Signer {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        let pair: SrPair = Pair::from_string(s, None).map_err(|err| anyhow!("{:?}", err))?;

        Ok(Self { pair })
    }
}

fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(pk: &Public, sig: P, message: M) -> Result<()> {
    validate_signature_len(&sig)?;

    let sig = sig.as_ref();

    if sig[0] != PREFIX {
        bail!("invalid signature type (expected sr25519)");
    }

    if !SrPair::verify_weak(&sig[1..], message, pk) {
        bail!("sr25519 signature verification failed")
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORDS: &str =
        "volume behind cable present pull exchange wish loyal avocado snap film increase";
    const SEED: &str = "0xb37231837527c7173dc212bb23a7fe795d5dae540e7c21366a5fb9f4cc398a36";

    #[test]
    fn test_load_from_mnemonics() {
        Sr25519Signer::try_from(WORDS).expect("key must be loaded");

        let err = Sr25519Signer::try_from("invalid words");
        assert_eq!(err.is_err(), true);
    }

    #[test]
    fn test_load_from_seed() {
        Sr25519Signer::try_from(SEED).expect("key must be loaded");

        let err = Sr25519Signer::try_from("0xinvalidseed");
        assert_eq!(err.is_err(), true);
    }
}
