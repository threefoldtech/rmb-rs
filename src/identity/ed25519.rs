use super::{validate_signature_len, Identity, Signer, SIGNATURE_LENGTH};
use anyhow::Result;
// use sp_core::{
//     crypto::AccountId32,
//     ed25519::{Pair as EdPair, Public},
//     Pair,
// };

use subxt::ext::{
    sp_core::{
        ed25519::{Pair as EdPair, Public},
        Pair,
    },
    sp_runtime::AccountId32,
};

use std::convert::From;
use tfchain_client::client::KeyPair;

pub const PREFIX: u8 = 0x65; // ascii e for ed

#[derive(Clone)]
pub struct Ed25519Identity(Public);

impl Identity for Ed25519Identity {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> Result<()> {
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
}

impl Signer for Ed25519Signer {
    fn sign<S: AsRef<[u8]>>(&self, msg: S) -> [u8; SIGNATURE_LENGTH] {
        let mut sig: [u8; SIGNATURE_LENGTH] = [0; SIGNATURE_LENGTH];
        sig[0] = PREFIX;
        sig[1..].clone_from_slice(&self.pair.sign(msg.as_ref()).0);

        sig
    }

    fn pair(&self) -> KeyPair {
        KeyPair::Ed25519(self.pair)
    }
}

impl Identity for Ed25519Signer {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> Result<()> {
        verify(&self.pair.public(), sig, message)
    }

    fn account(&self) -> AccountId32 {
        self.pair.public().into()
    }
}

impl TryFrom<&str> for Ed25519Signer {
    type Error = anyhow::Error;
    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        let pair: EdPair = Pair::from_string(s, None).map_err(|err| anyhow!("{:?}", err))?;

        Ok(Self { pair })
    }
}

fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(pk: &Public, sig: P, message: M) -> Result<()> {
    validate_signature_len(&sig)?;

    let sig = sig.as_ref();

    if sig[0] != PREFIX {
        bail!("invalid signature type (expected ed25519)");
    }

    if !EdPair::verify_weak(&sig[1..], message, pk) {
        bail!("ed25519 signature verification failed")
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORDS: &str = "neck stage box cup core magic produce exercise happy rely vocal then";
    const SEED: &str = "0xaa4e323bade8609a595108b585c4135855430c411ccf7923f81438cd8a188fce";

    #[test]
    fn test_load_from_mnemonics() {
        Ed25519Signer::try_from(WORDS).expect("key must be loaded");

        let err = Ed25519Signer::try_from("invalid words");
        assert_eq!(err.is_err(), true);
    }

    #[test]
    fn test_load_from_seed() {
        let _ = Ed25519Signer::try_from(SEED).expect("key must be loaded");

        let result = hex::decode(&SEED[2..SEED.len()]).expect("must be decoded");

        let _: [u8; 32] = result.as_slice().try_into().expect("key of size 32");

        let err = Ed25519Signer::try_from("0xinvalidseed");
        assert_eq!(err.is_err(), true);
    }
}
