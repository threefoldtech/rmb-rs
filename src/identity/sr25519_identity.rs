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
        let signature = self.pair.sign(msg.data.as_bytes());
        msg.signature = hex::encode(signature);
        Ok(msg)
    }

    fn verify<M: AsRef<str>>(&self, sig: &[u8], msg: M, pub_key: &[u8]) -> bool {
        todo!()
    }

    fn get_public_key(&self) -> String {
        todo!()
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
