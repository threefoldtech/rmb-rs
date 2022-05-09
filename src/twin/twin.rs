use anyhow::{Ok, Result};
// use sp_core::{Encode, Decode};

use crate::types::Message;
use async_trait::async_trait;
use rmb_client::RmbClient;
use sp_runtime::{MultiSignature, };

pub struct Twin<P>
where
    P: sp_core::Pair,
    MultiSignature: From<P::Signature>, 
{
    id: u64,
    address: String, // we use string not IP because the twin address can be a dns name
    client: RmbClient<P>,
}

impl<P> Twin<P>
where
    P: sp_core::Pair,
    MultiSignature: From<P::Signature>, 
{
    pub async fn new(id: u64, address: String, url: String) -> Result<Self> {
        let client = RmbClient::<P>::new(url)?;
        Ok(Twin { id, address, client })
    }
    pub async fn id(&self) -> u64 {
        self.id
    }

    pub async fn verify(&self, _msg: &Message) -> Result<()> {
        Ok(())
    }

    pub async fn address(&self) -> String {
        self.address.clone()
    }
}

#[async_trait]
impl<P> super::TwinDB for Twin<P>
where
    P: sp_core::Pair,
    MultiSignature: From<P::Signature>, 
{
    type T = Self;
    async fn get(&self, twin_id: u32) -> Result<Self::T> {
        let twin = self.client.get_twins(twin_id)?;
        Ok(twin)
    }


}
