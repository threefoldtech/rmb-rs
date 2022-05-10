mod twin;
mod substrate_twindb;

pub use twin::*;
pub use substrate_twindb::*;

use anyhow::Result;
use async_trait::async_trait;
use rmb_client::RmbClient;
use sp_core::{crypto::Dummy, ed25519, Pair};
use twin::Twin;

#[async_trait]
pub trait TwinDB {
    async fn get(&self, twin_id: u32) -> Result<Twin>;
}

pub struct SubstrateTwinDB {
    client: RmbClient<ed25519::Pair>,
}

impl SubstrateTwinDB {
    pub async fn new(url: String) -> Result<Self> {
        let client = RmbClient::<ed25519::Pair>::new("wss://tfchain.dev.grid.tf:443".to_string())?;

        Ok(Self { client })
    }
}

#[async_trait]
impl TwinDB for SubstrateTwinDB {
    async fn get(&self, twin_id: u32) -> Result<Twin> {
        let twin = self.client.get_twins(twin_id)?;
        Ok(twin)
    }
}
