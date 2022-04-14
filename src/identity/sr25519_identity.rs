use super::Identity;
use crate::types::Message;
use anyhow::Result;
use sp_core::{sr25519::Pair as SrPair, Pair};

#[derive(Clone)]
pub struct Sr25519Identity {
    pair: SrPair,
    #[allow(unused)]
    seed: [u8; 32],
}

impl Identity for Sr25519Identity {
    fn sign(&self, mut msg: Message) -> Result<Message> {
        let signature = self.pair.sign(msg.dat.as_bytes());
        msg.sig = hex::encode(signature);
        Ok(msg)
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
