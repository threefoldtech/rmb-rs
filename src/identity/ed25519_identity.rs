use super::Identity;
use crate::types::Message;
use anyhow::Result;
use sp_core::{ed25519::Pair as EdPair, Pair};

#[derive(Clone)]
pub struct Ed25519Identity {
    pair: EdPair,
    #[allow(unused)]
    seed: [u8; 32],
}

impl Identity for Ed25519Identity {
    fn sign(&self, mut msg: Message) -> Result<Message> {
        let signature = self.pair.sign(msg.dat.as_bytes());
        msg.sig = hex::encode(signature);
        Ok(msg)
    }
}

impl TryFrom<&str> for Ed25519Identity {
    type Error = anyhow::Error;
    fn try_from(phrase: &str) -> std::result::Result<Self, Self::Error> {
        // let (pair, seed) = sp_keyring::ed25519::Pair::from_phrase(phrase, None)?;
        let (pair, seed) = sp_core::ed25519::Pair::from_phrase(phrase, None)
            .map_err(|err| anyhow!("{:?}", err))?;

        Ok(Self { pair, seed })
    }
}

// [0, 1, 2] <[u8]> -> string ?! 0 , 1 , 2
