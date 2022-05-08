use std::str::FromStr;

use anyhow::{Ok, Result};
use codec::{Decode, Encode};
use sp_core::Pair;
use sp_runtime::MultiSignature;
use substrate_api_client::Api;

pub struct Client<P>
where
    P: Pair,
    MultiSignature: From<P::Signature>,
{
    api: Api<P>,
}

impl<P> Client<P>
where
    P: Pair,
    MultiSignature: From<P::Signature>,
{
    pub fn new(url: String) -> Result<Self> {
        let api = Api::<P>::new(url)?;
        Ok(Self { api })
    }

    pub fn get_twins<T: Decode>(&self, id: u32) -> Result<T> {
        let twin: T = self
            .api
            .get_storage_map("TfgridModule", "Twins", id.encode(), None)?
            .ok_or(anyhow::anyhow!("Twin id is not found"))?
            .decode()?;

        Ok(twin)
    }

    pub fn get_twin_id_by_account_id(&self, account_id: String) -> Result<u32> {
        let account_id = sp_core::ed25519::Public::from_str(&account_id)
            .map_err(|err| anyhow::anyhow!(format!("{:?}", err)))?;

        let twin_id: u32 = self
            .api
            .get_storage_map(
                "TfgridModule",
                "TwinIdByAccountID",
                account_id.encode(),
                None,
            )?
            .ok_or(anyhow::anyhow!("Account id is not found"))?
            .decode()?;

        Ok(twin_id)
    }
}
