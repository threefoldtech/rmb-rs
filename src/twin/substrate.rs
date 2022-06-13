use super::Twin;
use super::TwinDB;
use crate::cache::Cache;
use anyhow::Result;
use async_trait::async_trait;
use sp_core::crypto::AccountId32;
use std::sync::Arc;
use substrate_client::SubstrateClient;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use workers::{Work, WorkerPool};

const MAX_INFLIGHT: usize = 10;

#[derive(Clone)]
struct TwinGetter<C>
where
    C: Cache<Twin>,
{
    client: SubstrateClient,
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
        // not we hit the cache again so if may requests to query the
        // same twin and they were "blocked" waiting for a turn to
        // execute the query. There might be already someone who
        // have populated the cache with this value. Otherwise
        // we need to do the actual query.
        // this looks ugly we have to hit the cache 2 times for
        // twins that are not in cache but overall performance is
        // improved.
        if let Some(twin) = self.cache.get(twin_id).await? {
            return Ok(Some(twin));
        }

        let client = self.client.clone();
        log::debug!("getting twin {} from substrate", twin_id);
        spawn_blocking(move || client.get_twin(twin_id)).await?
    }
}

#[derive(Clone)]
pub struct SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    pool: Arc<Mutex<WorkerPool<TwinGetter<C>>>>,
    client: SubstrateClient,
    cache: C,
}

impl<C> SubstrateTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    pub fn new<S: Into<String>>(url: S, cache: C) -> Result<Self> {
        let client = SubstrateClient::new(url.into())?;
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

    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<u32> {
        let client = self.client.clone();
        let twin_id: u32 =
            spawn_blocking(move || client.get_twin_id_by_account_id(account_id)).await??;
        Ok(twin_id)
    }
}

#[cfg(test)]
mod tests {

    use crate::cache::{MemCache, NoCache};

    use super::*;
    use anyhow::Context;
    use sp_core::crypto::Ss58Codec;

    #[tokio::test]
    async fn test_get_twin_with_mem_cache() {
        let mem: MemCache<Twin> = MemCache::new();

        let db = SubstrateTwinDB::new("wss://tfchain.dev.grid.tf", Some(mem.clone()))
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
        let db = SubstrateTwinDB::new("wss://tfchain.dev.grid.tf", NoCache)
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
        let db = SubstrateTwinDB::new("wss://tfchain.dev.grid.tf", NoCache)
            .context("cannot create substrate twin db object")
            .unwrap();

        // let identity = Ed25519Identity::try_from("mnemonics").unwrap();
        // let account_id = identity.get_public_key();
        let account_id =
            AccountId32::from_ss58check("5EyHmbLydxX7hXTX7gQqftCJr2e57Z3VNtgd6uxJzZsAjcPb")
                .unwrap();

        let twin_id = db
            .get_twin_with_account(account_id)
            .await
            .context("can't get twin from substrate")
            .unwrap();

        assert_eq!(55, twin_id);
    }
}
