use super::Identity;
use crate::types::Message;
use anyhow::{Ok, Result};
use sp_core::{sr25519::Pair as SrPair, Pair};

#[derive(Clone)]
pub struct Sr25519Identity {
    pair: SrPair,
    #[allow(unused)]
    seed: [u8; 32],
}

impl Identity for Sr25519Identity {
    fn id(&self) -> u64 {
        let _key = self.pair.public();
        todo!("Check the return value")
    }
    fn sign(&self, msg: Message) -> Result<Message> {
        let mut msg = msg;
        let signed_msg = self.pair.sign(msg.dat.as_bytes()).0;
        msg.dat = String::from_utf8(signed_msg.to_vec())?;
        Ok(msg)
    }
}

impl TryFrom<&str> for Sr25519Identity {
    type Error = anyhow::Error;

    fn try_from(phrase: &str) -> Result<Self, Self::Error> {
        // let (pair, seed) = sp_keyring::sr25519::Pair::from_phrase(phrase, None)?;
        let (pair, seed) = sp_core::sr25519::Pair::from_phrase(phrase, None)
            .map_err(|err| anyhow::anyhow!("{:?}", err))?;

        Ok(Self { pair, seed })
    }
}
