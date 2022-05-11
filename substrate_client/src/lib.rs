use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use codec::{Decode, Encode};
use sp_core::ed25519;
use substrate_api_client::Api;

#[derive(Clone)]
pub struct SubstrateClient {
    api: Arc<Api<ed25519::Pair>>,
}

impl SubstrateClient {
    pub fn new(url: String) -> Result<Self> {
        let api =
            Arc::new(Api::<ed25519::Pair>::new(url).context("failed to create substrate client")?);
        Ok(Self { api })
    }

    pub fn get_twin<T: Decode>(&self, id: u32) -> Result<T> {
        let twin: T = self
            .api
            .get_storage_map("TfgridModule", "Twins", id.encode(), None)?
            .ok_or(anyhow::anyhow!("twin id is not found"))?
            .decode()
            .context("failed to decode twin object")?;

        Ok(twin)
    }

    pub fn get_twin_id_by_account_id(&self, account_id: String) -> Result<u32> {
        let account_id = sp_core::ed25519::Public::from_str(&account_id)
            .map_err(|err| anyhow::anyhow!(format!("{:?}", err)))
            .context("failed to get ed25519 key from account id")?;

        let twin_id: u32 = self
            .api
            .get_storage_map(
                "TfgridModule",
                "TwinIdByAccountID",
                account_id.encode(),
                None,
            )?
            .ok_or(anyhow::anyhow!("Account id is not found"))?
            .decode()
            .context("failed to decode twin object")?;

        Ok(twin_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_twin_id() {
        let client = SubstrateClient::<sp_core::ed25519::Pair>::new(
            "wss://tfchain.dev.grid.tf:443".to_string(),
        )
        .unwrap();
        println!("{:#?}", client.get_twin(55));
        assert_eq!(
            55,
            client
                .get_twin_id_by_account_id(
                    "5EyHmbLydxX7hXTX7gQqftCJr2e57Z3VNtgd6uxJzZsAjcPb".to_string()
                )
                .unwrap()
        );
    }
}
