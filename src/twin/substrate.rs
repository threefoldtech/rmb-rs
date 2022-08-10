use super::Twin;
use super::TwinDB;
use crate::cache::Cache;
use anyhow::Result;
use async_trait::async_trait;
use sp_core::crypto::AccountId32;
use std::sync::Arc;
use subxt::{
    storage::StorageEntry, ClientBuilder, DefaultConfig, StorageEntryKey, StorageHasher,
    StorageMapKey,
};
use tokio::sync::Mutex;

use workers::{Work, WorkerPool};

const MAX_INFLIGHT: usize = 10;

struct TwinID {
    id: u32,
}

impl TwinID {
    pub fn new(id: u32) -> Self {
        Self { id }
    }
}

impl StorageEntry for TwinID {
    type Value = Twin;
    const PALLET: &'static str = "TfgridModule";
    const STORAGE: &'static str = "Twins";

    fn key(&self) -> StorageEntryKey {
        let args = vec![StorageMapKey::new(
            &self.id,
            StorageHasher::Blake2_128Concat,
        )];
        StorageEntryKey::Map(args)
    }
}

pub struct TwinAccountID {
    id: AccountId32,
}

impl TwinAccountID {
    pub fn new(id: AccountId32) -> Self {
        Self { id }
    }
}

impl StorageEntry for TwinAccountID {
    type Value = u32;
    const PALLET: &'static str = "TfgridModule";
    const STORAGE: &'static str = "TwinIdByAccountID";

    fn key(&self) -> StorageEntryKey {
        let args = vec![StorageMapKey::new(
            &self.id,
            StorageHasher::Blake2_128Concat,
        )];
        StorageEntryKey::Map(args)
    }
}

type Client = subxt::Client<DefaultConfig>;

#[derive(Debug, Clone)]
struct ReconnectingClient {
    client: Arc<Mutex<Client>>,
    url: String,
}

impl ReconnectingClient {
    async fn get_new(url: &str) -> Result<Client> {
        Ok(ClientBuilder::new().set_url(url).build().await?)
    }

    pub async fn new<S: Into<String>>(url: S) -> Result<Self> {
        let url = url.into();

        let client = Self::get_new(&url).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            url,
        })
    }

    pub async fn get(&self) -> Result<Client> {
        let mut client = self.client.lock().await;

        if !client.rpc().client.is_connected() {
            log::debug!("client is disconnected, rebuilding a new one");
            *client = Self::get_new(&self.url).await?;
        }

        Ok(client.clone())
    }
}

#[derive(Clone)]
struct TwinGetter<C>
where
    C: Cache<Twin>,
{
    client: ReconnectingClient,
    cache: C,
}

#[async_trait::async_trait]
impl<C> Work for TwinGetter<C>
where
    C: Cache<Twin>,
{
    type Input = u32;
    type Output = Result<Option<Twin>>;

    async fn run(&self, twin_id: Self::Input) -> Self::Output {
        // note: we hit the cache again so if many requests are querying the
        // same twin and they were "blocked" waiting for a turn to
        // execute the query, there might be already someone who
        // have populated the cache with this value. Otherwise
        // we need to do the actual query.
        // this looks ugly we have to hit the cache 2 times for
        // twins that are not in cache but overall performance is
        // improved.
        if let Some(twin) = self.cache.get(twin_id).await? {
            return Ok(Some(twin));
        }

        let client = self.client.get().await?;
        Ok(client.storage().fetch(&TwinID::new(twin_id), None).await?)
    }
}

#[derive(Clone)]
pub struct SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    pool: Arc<Mutex<WorkerPool<TwinGetter<C>>>>,
    client: ReconnectingClient,
    cache: C,
}

impl<C> SubstrateTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    pub async fn new<S: Into<String>>(url: S, cache: C) -> Result<Self> {
        let client = ReconnectingClient::new(url).await?;
        let work = TwinGetter {
            client: client.clone(),
            cache: cache.clone(),
        };
        let pool = Arc::new(Mutex::new(WorkerPool::new(work, MAX_INFLIGHT)));
        Ok(Self {
            pool,
            client,
            cache,
        })
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

        // but if we wanna hit the grid we get throttled by the workers pool
        // the pool has a limited size so only X queries can be in flight.
        let worker = self.pool.lock().await.get().await;

        let twin = worker.run(twin_id).await??;
        if let Some(ref twin) = twin {
            self.cache.set(twin.id, twin.clone()).await?;
        }

        Ok(twin)
    }

    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>> {
        let client = self.client.get().await?;
        Ok(client
            .storage()
            .fetch(&TwinAccountID::new(account_id), None)
            .await?)
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

        let db = SubstrateTwinDB::new("wss://tfchain.dev.grid.tf:443", Some(mem.clone()))
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
        let db = SubstrateTwinDB::new("wss://tfchain.dev.grid.tf:443", NoCache)
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
        assert_eq!(twin.address, "::11");
        assert_eq!(
            twin.account.to_string(),
            "5Eh2stFNQX4khuKoh2a1jQBVE91Lv3kyJiVP2Y5webontjRe"
        );
    }

    #[tokio::test]
    async fn test_get_twin_id() {
        let db = SubstrateTwinDB::new("wss://tfchain.dev.grid.tf:443", NoCache)
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
