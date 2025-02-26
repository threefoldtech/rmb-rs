use super::{validate_signature_len, Identity, Signer, SIGNATURE_LENGTH};
use anyhow::Result;

use hex::decode;
use sp_core::crypto::{AccountId32, CryptoBytes};
use sp_core::{
    sr25519::{Pair as SrPair, Public},
    Pair,
};
use std::convert::From;

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

    fn try_from(private_key_hex: &str) -> std::result::Result<Self, Self::Error> {
        let private_key_bytes = decode(private_key_hex)?;
        let private_key_array: [u8; 64] = private_key_bytes
            .try_into()
            .expect("Failed to convert to fixed-size array");

        // Extract the first 32 bytes as the secret key
        let secret_key = &private_key_array[0..32];

        let pair: SrPair = SrPair::from_seed_slice(secret_key)?;
        Ok(Self { pair })
    }
}

fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(pk: &Public, sig: P, message: M) -> Result<()> {
    validate_signature_len(&sig)?;

    let sig = sig.as_ref();

    if sig[0] != PREFIX {
        bail!("invalid signature type (expected sr25519)");
    }

    let crypto_signature: CryptoBytes<64, _> =
        sig[1..].try_into().expect("invalid signature length");
    if !SrPair::verify(&crypto_signature, message, pk) {
        bail!("sr25519 signature verification failed")
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIVATE_KEY: &str = "56ad3b7b0925fbb9b6d43f8db2a9b199b28c4c8cfdbbf8ecbfb5f20dfd09009e15f85563edc6e9b456b31e7d7cf720c7d3d897cc54ef61c28f3a0d52de9296b1";

    #[test]
    fn test_load_from_private_key() {
        Sr25519Signer::try_from(PRIVATE_KEY).expect("key must be loaded");

        let err = Sr25519Signer::try_from("invalid");
        assert_eq!(err.is_err(), true);
    }
}
