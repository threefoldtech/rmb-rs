mod substrate;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
pub use substrate::*;
use subxt::utils::AccountId32;

#[async_trait]
pub trait TwinDB: Send + Sync + 'static {
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>>;
    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>>;
}

use tfchain_client::client::Twin as TwinData;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Twin {
    pub id: u32,
    pub account: AccountId32,
    pub relay: Option<String>,
    pub pk: Option<Vec<u8>>,
}

impl From<TwinData> for Twin {
    fn from(twin: TwinData) -> Self {
        Twin {
            id: twin.id,
            account: twin.account_id,
            relay: twin.relay.map(|v| String::from_utf8_lossy(&v.0).into()),
            pk: twin.pk.map(|v| v.0),
        }
    }
}
