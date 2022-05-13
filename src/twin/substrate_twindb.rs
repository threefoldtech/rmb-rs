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
    pub async fn new<S: Into<String>>(url: S, cache: Option<C>) -> Result<Self> {
        let client = SubstrateClient::new(url.into())?;
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

        let db =
            SubstrateTwinDB::<MemCache<Twin>>::new("wss://tfchain.dev.grid.tf", Some(mem.clone()))
                .await
                .context("cannot create substrate twin db object")
                .unwrap();

        let twin = db
            .get(1)
            .await
            .context("can't get twin from substrate")
            .unwrap();

        // NOTE: this currently checks against devnet substrate
        // as provided by the url wss://tfchain.dev.grid.tf.
        // if this environment was reset at some point. those
        // values won't match anymore.
        assert_eq!(twin.address, "::11");
        assert_eq!(
            twin.account.to_string(),
            "5Eh2stFNQX4khuKoh2a1jQBVE91Lv3kyJiVP2Y5webontjRe"
        );

        let cached_twin = mem
            .get::<u32>(1)
            .await
            .context("cannot get twin from the cache")
            .unwrap();

        assert_eq!(Some(twin), cached_twin);
    }

    #[tokio::test]
    async fn test_with_no_cache() {
        let db = SubstrateTwinDB::<MemCache<Twin>>::new("wss://tfchain.dev.grid.tf", None)
            .await
            .context("cannot create substrate twin db object")
            .unwrap();

        let twin = db
            .get(1)
            .await
            .context("can't get twin from substrate")
            .unwrap();

        // NOTE: this currently checks against devnet substrate
        // as provided by the url wss://tfchain.dev.grid.tf.
        // if this environment was reset at some point. those
        // values won't match anymore.
        assert_eq!(twin.address, "::11");
        assert_eq!(
            twin.account.to_string(),
            "5Eh2stFNQX4khuKoh2a1jQBVE91Lv3kyJiVP2Y5webontjRe"
        );
    }
}
