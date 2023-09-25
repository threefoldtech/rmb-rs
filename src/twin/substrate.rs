use super::RelayDomains;
use super::Twin;
use super::TwinDB;
use crate::cache::Cache;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::LinkedList;
use std::sync::Arc;
use subxt::utils::AccountId32;
use subxt::Error as ClientError;
use tokio::sync::Mutex;

use tfchain_client::client::{Client, Hash, KeyPair};

#[derive(Clone)]
pub struct SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    client: Arc<Mutex<ClientWrapper>>,
    cache: C,
}

impl<C> SubstrateTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    pub async fn new(substrate_urls: Vec<String>, cache: C) -> Result<Self> {
        let client_wrapper = ClientWrapper::new(substrate_urls).await?;

        Ok(Self {
            client: Arc::new(Mutex::new(client_wrapper)),
            cache,
        })
    }

    pub async fn update_twin(
        &self,
        kp: &KeyPair,
        relay: RelayDomains,
        pk: Option<&[u8]>,
    ) -> Result<Hash> {
        let mut client = self.client.lock().await;
        let hash = client.update_twin(kp, Some(relay.to_string()), pk).await?;
        Ok(hash)
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

        let twin = client.get_twin_by_id(twin_id).await?;

        // but if we wanna hit the grid we get throttled by the workers pool
        // the pool has a limited size so only X queries can be in flight.

        if let Some(ref twin) = twin {
            self.cache.set(twin.id, twin.clone()).await?;
        }

        Ok(twin)
    }

    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>> {
        let mut client = self.client.lock().await;

        let id = client.get_twin_id_by_account(account_id).await?;

        Ok(id)
    }
}

/// ClientWrapper is basically a substrate client.
/// all methods exported by the ClientWrapper has a reconnect functionality internally.
/// so if any network error happened, the ClientWrapper will try to reconnect to substrate using the provided substrate urls.
/// if after a number of trials (currently 2 * the number of urls) a reconnect was not successful, the ClientWrapper returns an error.
struct ClientWrapper {
    client: Client,
    substrate_urls: LinkedList<String>,
}

impl ClientWrapper {
    pub async fn new(substrate_urls: Vec<String>) -> Result<Self> {
        let mut urls = LinkedList::new();
        for url in substrate_urls {
            urls.push_back(url);
        }

        let client = Self::connect(&mut urls).await?;

        Ok(Self {
            client,
            substrate_urls: urls,
        })
    }

    async fn connect(urls: &mut LinkedList<String>) -> Result<Client> {
        let trials = urls.len() * 2;
        for _ in 0..trials {
            let url = match urls.front() {
                Some(url) => url,
                None => anyhow::bail!("substrate urls list is empty"),
            };

            match Client::new(&url).await {
                Ok(client) => return Ok(client),
                Err(err) => {
                    log::error!(
                        "failed to create substrate client with url \"{}\": {}",
                        url,
                        err
                    );
                }
            }

            if let Some(front) = urls.pop_front() {
                urls.push_back(front);
            }
        }

        anyhow::bail!("failed to connect to substrate using the provided urls")
    }

    pub async fn update_twin(
        &mut self,
        kp: &KeyPair,
        relay: Option<String>,
        pk: Option<&[u8]>,
    ) -> Result<Hash> {
        let hash = loop {
            match self.client.update_twin(kp, relay.clone(), pk).await {
                Ok(hash) => break hash,
                Err(ClientError::Rpc(_)) => {
                    self.client = Self::connect(&mut self.substrate_urls).await?;
                }
                Err(err) => return Err(err.into()),
            }
        };

        Ok(hash)
    }

    pub async fn get_twin_by_id(&mut self, twin_id: u32) -> Result<Option<Twin>> {
        let twin = loop {
            match self.client.get_twin_by_id(twin_id).await {
                Ok(twin) => break twin,
                Err(ClientError::Rpc(_)) => {
                    self.client = Self::connect(&mut self.substrate_urls).await?;
                }
                Err(err) => return Err(err.into()),
            }
        }
        .map(Twin::from);

        Ok(twin)
    }

    pub async fn get_twin_id_by_account(&mut self, account_id: AccountId32) -> Result<Option<u32>> {
        let id = loop {
            match self.client.get_twin_id_by_account(account_id.clone()).await {
                Ok(twin) => break twin,
                Err(ClientError::Rpc(_)) => {
                    self.client = Self::connect(&mut self.substrate_urls).await?;
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
            vec![String::from("wss://tfchain.dev.grid.tf:443")],
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
        let db = SubstrateTwinDB::new(vec![String::from("wss://tfchain.dev.grid.tf:443")], NoCache)
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
        let db = SubstrateTwinDB::new(vec![String::from("wss://tfchain.dev.grid.tf:443")], NoCache)
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

    #[tokio::test]
    async fn test_multiple_substrate_urls() {
        SubstrateTwinDB::new(
            vec![
                String::from("wss://invalid_substrate_url_1:443"),
                String::from("wss://invalid_substrate_url_1:443"),
                String::from("wss://tfchain.dev.grid.tf:443"),
            ],
            NoCache,
        )
        .await
        .context("cannot create substrate twin db object")
        .unwrap();
    }
}
