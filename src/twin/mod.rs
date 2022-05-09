mod twin;

use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait TwinDB {
    type T;
    async fn get(&self, twin_id: u32) -> Result<Self::T>;
}