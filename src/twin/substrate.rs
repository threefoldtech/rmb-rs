use super::Twin;
use super::TwinDB;
use crate::cache::Cache;
use anyhow::Result;
use async_trait::async_trait;
use rand::seq::SliceRandom;
use std::sync::Arc;
use subxt::utils::AccountId32;
use subxt::Error as ClientError;
use tokio::sync::Mutex;

use tfchain_client::client::{Client, KeyPair};

#[derive(Clone)]
pub struct SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    substrate_urls: Arc<Vec<String>>,
    client: Arc<Mutex<Client>>,
    cache: C,
}

impl<C> SubstrateTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    pub async fn new(substrate_urls: Arc<Vec<String>>, cache: C) -> Result<Self> {
        let client = Self::connect(substrate_urls.clone()).await?;
        Ok(Self {
            substrate_urls: substrate_urls.clone(),
            client: Arc::new(Mutex::new(client)),
            cache,
        })
    }

    async fn connect(urls: Arc<Vec<String>>) -> Result<Client> {
        let url = urls
            .choose(&mut rand::thread_rng())
            .ok_or(ClientError::Other(String::from(
                "failed to choose substrate url",
            )))?;
        let client = Client::new(&url).await?;
        Ok(client)
    }

    pub async fn update_twin(
        &self,
        kp: &KeyPair,
        relay: Option<String>,
        pk: Option<&[u8]>,
    ) -> Result<()> {
        let client = self.client.lock().await;
        let hash = client.update_twin(kp, relay, pk).await?;
        log::debug!("hash: {:?}", hash);
        Ok(())
    }
}

#[async_trait]
impl<C> TwinDB for SubstrateTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>> {
        // we can hit the cache as fast as we can here
        if let Some(twin) = self.cache.get(twin_id).await? {
            return Ok(Some(twin));
        }

        let mut client = self.client.lock().await;

        let twin = loop {
            match client.get_twin_by_id(twin_id).await {
                Ok(twin) => break twin,
                Err(ClientError::Rpc(_)) => {
                    *client = Self::connect(self.substrate_urls.clone()).await?;
                }
                Err(err) => return Err(err.into()),
            }
        }
        .map(Twin::from);

        // but if we wanna hit the grid we get throttled by the workers pool
        // the pool has a limited size so only X queries can be in flight.

        if let Some(ref twin) = twin {
            self.cache.set(twin.id, twin.clone()).await?;
        }

        Ok(twin)
    }

    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>> {
        let mut client = self.client.lock().await;

        let id = loop {
            match client.get_twin_id_by_account(account_id.clone()).await {
                Ok(twin) => break twin,
                Err(ClientError::Rpc(_)) => {
                    *client = Self::connect(self.substrate_urls.clone()).await?;
                }
                Err(err) => return Err(err.into()),
            }
        };

        Ok(id)
    }
}

#[cfg(test)]
mod tests {

    use crate::cache::{MemCache, NoCache};

    use super::*;
    use anyhow::Context;

    #[tokio::test]
    async fn test_get_twin_with_mem_cache() {
        let mem: MemCache<Twin> = MemCache::new();

        let db = SubstrateTwinDB::new(
            Arc::new(vec![String::from("wss://tfchain.dev.grid.tf:443")]),
            Some(mem.clone()),
        )
        .await
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
        assert!(matches!(twin.relay, None));
        assert!(matches!(twin.pk, None));
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
        let db = SubstrateTwinDB::new(
            Arc::new(vec![String::from("wss://tfchain.dev.grid.tf:443")]),
            NoCache,
        )
        .await
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
        assert!(matches!(twin.relay, None));
        assert!(matches!(twin.pk, None));
        assert_eq!(
            twin.account.to_string(),
            "5Eh2stFNQX4khuKoh2a1jQBVE91Lv3kyJiVP2Y5webontjRe"
        );
    }

    #[tokio::test]
    async fn test_get_twin_id() {
        let db = SubstrateTwinDB::new(
            Arc::new(vec![String::from("wss://tfchain.dev.grid.tf:443")]),
            NoCache,
        )
        .await
        .context("cannot create substrate twin db object")
        .unwrap();

        let account_id: AccountId32 = "5EyHmbLydxX7hXTX7gQqftCJr2e57Z3VNtgd6uxJzZsAjcPb"
            .parse()
            .unwrap();

        let twin_id = db
            .get_twin_with_account(account_id)
            .await
            .context("can't get twin from substrate")
            .unwrap();

        assert_eq!(Some(55), twin_id);
    }
}
