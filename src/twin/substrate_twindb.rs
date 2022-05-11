use crate::cache::Cache;
use crate::cache::RedisCache;
use anyhow::Result;
use async_trait::async_trait;
use sp_core::ed25519;
use substrate_client::SubstrateClient;
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
    client: SubstrateClient,
    cache: Option<C>,
}

impl<C> SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    pub async fn new(url: String, cache: Option<C>) -> Result<Self> {
        let client = SubstrateClient::new(url)?;
        Ok(Self { client, cache })
    }


    async fn get_cached_twin(&self, twin_id: u32) -> Option<Twin> {
        if let Some(cache) = self.cache.clone() {

            if let Ok(res) = cache.get(twin_id).await {
                return res;
            }
        }

        None
    }
    
}

#[async_trait]
impl<C> TwinDB for SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    async fn get(&self, twin_id: u32) -> Result<Option<Twin>> {

        match self.get_cached_twin(twin_id).await {
            Some(twin) => Ok(Some(twin)),
            None => {
                let client = self.client.clone();
                spawn_blocking(move || client.get_twin(twin_id)).await?
            }
        }
    }
}
