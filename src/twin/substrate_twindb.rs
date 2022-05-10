use crate::cache::Cache;
use crate::cache::RedisCache;
use anyhow::Result;
use async_trait::async_trait;
use rmb_client::RmbClient;
use sp_core::ed25519;
use tokio::task::spawn_blocking;

use super::Twin;
use super::TwinDB;

// async fn how_to_init() {
//     let c = RedisCache::new("redis://localhost".to_string()).await.unwrap();
//     let s = SubstrateTwinDB::new("url_to_substrate".to_string(), Some(c)).await.unwrap();
// }
pub struct SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    client: RmbClient<ed25519::Pair>,
    cache: Option<C>,
}

impl<C> SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    pub async fn new(url: String, cache: Option<C>) -> Result<Self> {
        let client = RmbClient::<ed25519::Pair>::new(url)?;
        Ok(Self { client, cache })
    }
}

#[async_trait]
impl<C> TwinDB for SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    async fn get(&self, twin_id: u32) -> Result<Twin> {
        let client = self.client.clone();
        let twin = spawn_blocking(move || client.get_twin(twin_id)).await??;
        Ok(twin)
    }
}
