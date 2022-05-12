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
}

#[async_trait]
impl<C> TwinDB for SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    async fn get(&self, twin_id: u32) -> Result<Twin> {
        if let Some(twin) = self.cache.get(twin_id).await? {
            return Ok(twin);
        }

        let client = self.client.clone();
        let twin: Twin = spawn_blocking(move || client.get_twin(twin_id)).await??;
        self.cache.set(twin.id, twin.clone()).await?;
        Ok(twin)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use crate::cache::MemCache;

    use super::*;
    use anyhow::Context;

    #[tokio::test]
    async fn test_with_mem_cache() {
        let mem: MemCache<Twin> = MemCache::new();

        let db = SubstrateTwinDB::<MemCache<Twin>>::new("url".to_string(), Some(mem.clone()))
            .await
            .context("cannot create substrate twin db object")
            .unwrap();

        let twin = db
            .get(55)
            .await
            .context("can't get twin from substrate")
            .unwrap();

        let cached_twin = mem
            .get::<u32>(55)
            .await
            .context("cannot get twin from the cache")
            .unwrap();

        assert_eq!(Some(twin), cached_twin);
    }
}
