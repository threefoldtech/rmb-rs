use crate::cache::Cache;
use crate::cache::RedisCache;
use anyhow::Result;
use async_trait::async_trait;
use sp_core::ed25519;
use substrate_client::SubstrateClient;
use tokio::task::spawn_blocking;

use super::Twin;
use super::TwinDB;

#[derive(Clone)]
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
    pub fn new<S: Into<String>>(url: S, cache: Option<C>) -> Result<Self> {
        let client = SubstrateClient::new(url.into())?;
        Ok(Self { client, cache })
    }
}

#[async_trait]
impl<C> TwinDB for SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>> {
        if let Some(twin) = self.cache.get(twin_id).await? {
            return Ok(Some(twin));
        }

        let client = self.client.clone();
        let twin: Twin = spawn_blocking(move || client.get_twin(twin_id)).await??;
        self.cache.set(twin.id, twin.clone()).await?;
        Ok(Some(twin))
    }

    async fn get_twin_id<S: Into<String> + Send + 'static>(&self, account_id: S) -> Result<u32> {
        let client = self.client.clone();
        let twin_id: u32 =
            spawn_blocking(move || client.get_twin_id_by_account_id(account_id.into())).await??;
        Ok(twin_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use crate::{cache::MemCache, identity::Ed25519Identity};

    use super::*;
    use anyhow::Context;

    #[tokio::test]
    async fn test_get_twin_with_mem_cache() {
        let mem: MemCache<Twin> = MemCache::new();

        let db =
            SubstrateTwinDB::<MemCache<Twin>>::new("wss://tfchain.dev.grid.tf", Some(mem.clone()))
                .context("cannot create substrate twin db object")
                .unwrap();

        let twin = db
            .get_twin(1)
            .await
            .context("can't get twin from substrate")
            .unwrap()
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
    async fn test_get_twin_with_no_cache() {
        let db = SubstrateTwinDB::<MemCache<Twin>>::new("wss://tfchain.dev.grid.tf", None)
            .context("cannot create substrate twin db object")
            .unwrap();

        let twin = db
            .get_twin(1)
            .await
            .context("can't get twin from substrate")
            .unwrap()
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

    #[ignore]
    #[tokio::test]
    async fn test_get_twin_id() {
        let db = SubstrateTwinDB::<MemCache<Twin>>::new("wss://tfchain.dev.grid.tf", None)
            .context("cannot create substrate twin db object")
            .unwrap();

        // let identity = Ed25519Identity::try_from("mnemonics").unwrap();
        // let account_id = identity.get_public_key();

        let account_id = "5EyHmbLydxX7hXTX7gQqftCJr2e57Z3VNtgd6uxJzZsAjcPb".to_string();

        let twin_id = db
            .get_twin_id(account_id)
            .await
            .context("can't get twin from substrate")
            .unwrap();

        assert_eq!(55, twin_id);
    }
}
