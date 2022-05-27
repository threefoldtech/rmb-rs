use super::{Identity, Signer};
use crate::types::Message;
use anyhow::Result;
use sp_core::{sr25519::Pair as SrPair, Pair};

#[derive(Clone)]
pub struct Sr25519Identity {
    pair: SrPair,
    #[allow(unused)]
    seed: [u8; 32],
}

impl Signer for Sr25519Identity {
    fn sign<S: AsRef<[u8]>>(&self, msg: S) -> [u8; 64] {
        self.pair.sign(msg.as_ref()).0
    }
}

impl Identity for Sr25519Identity {
    fn verify<P: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: P, message: M) -> bool {
        SrPair::verify_weak(sig.as_ref(), message, self.pair.public())
    }

    fn get_public_key(&self) -> String {
        self.pair.public().to_string()
    }
}

impl TryFrom<&str> for Sr25519Identity {
    type Error = anyhow::Error;

    fn try_from(phrase: &str) -> std::result::Result<Self, Self::Error> {
        // let (pair, seed) = sp_keyring::sr25519::Pair::from_phrase(phrase, None)?;
        let (pair, seed) = sp_core::sr25519::Pair::from_phrase(phrase, None)
            .map_err(|err| anyhow::anyhow!("{:?}", err))?;

        Ok(Self { pair, seed })
    }
}
