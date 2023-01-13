mod substrate;

use anyhow::Result;
use async_trait::async_trait;

pub use substrate::*;
use subxt::ext::sp_runtime::AccountId32;

#[async_trait]
pub trait TwinDB: Send + Sync + 'static {
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>>;
    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>>;
}

pub use tfchain_client::client::Twin;
