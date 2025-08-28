use super::RelayDomains;
use super::Twin;
use super::TwinDB;
use super::TwinID;
use crate::cache::Cache;
use anyhow::{anyhow, Result};
use arc_swap::ArcSwap;
use futures::future::{BoxFuture, FutureExt, Shared};
use std::collections::{HashMap, LinkedList};
use std::sync::Arc;
use std::time::Duration;
use subxt::utils::AccountId32;
use subxt::Error as ClientError;
use tokio::sync::Mutex;
use tokio::time::timeout;

use tfchain_client::client::{Client, Hash, KeyPair};

// Type aliases to simplify complex generic types used for singleflight fetches
type TwinFetchResult = Result<Arc<Option<Twin>>, Arc<anyhow::Error>>;
type TwinFetchFuture = Shared<BoxFuture<'static, TwinFetchResult>>;
type InFlightMap = HashMap<u32, TwinFetchFuture>;

/// Try to establish a client connection with per-attempt timeout and URL rotation.
async fn connect_with_timeouts(urls: &Arc<Mutex<LinkedList<String>>>) -> Result<Client> {
    // Try up to 2 * urls.len() attempts, rotating on each failure
    let trials = {
        let urls_guard = urls.lock().await;
        let len = urls_guard.len();
        if len == 0 {
            return Err(anyhow!("substrate urls list is empty"));
        }
        len * 2
    };

    for _ in 0..trials {
        let current_url = {
            let urls_guard = urls.lock().await;
            urls_guard.front().cloned()
        };

        let url = match current_url {
            Some(u) => u,
            None => return Err(anyhow!("substrate urls list is empty")),
        };

        match timeout(Duration::from_secs(12), Client::new(&url)).await {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(err)) => {
                log::error!(
                    "failed to create substrate client with url \"{}\": {}",
                    url,
                    err
                );
            }
            Err(_) => {
                log::error!(
                    "timeout while creating substrate client with url \"{}\"",
                    url
                );
            }
        }

        // rotate URL
        let mut urls_guard = urls.lock().await;
        if let Some(front) = urls_guard.pop_front() {
            urls_guard.push_back(front);
        }
    }

    Err(anyhow!(
        "failed to connect to substrate using the provided urls"
    ))
}

#[derive(Clone)]
pub struct SubstrateTwinDB<C>
where
    C: Cache<Twin>,
{
    // Lock-free snapshot for reads; atomic swap on reconnect
    client: Arc<ArcSwap<Client>>,
    // URL rotation list protected by a small mutex
    substrate_urls: Arc<Mutex<LinkedList<String>>>,
    // Singleflight for reconnects
    reconnect_lock: Arc<Mutex<()>>,
    // In-flight fetches for cache miss de-duplication. Output must be Clone for `shared()`
    // so we wrap both Ok and Err in Arc<...>.
    in_flight: Arc<Mutex<InFlightMap>>,
    cache: C,
}

impl<C> SubstrateTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    pub async fn new(substrate_urls: Vec<String>, cache: C) -> Result<Self> {
        let mut urls_list = LinkedList::new();
        for url in substrate_urls {
            urls_list.push_back(url);
        }

        let urls = Arc::new(Mutex::new(urls_list));
        let client = connect_with_timeouts(&urls).await?;

        Ok(Self {
            client: Arc::new(ArcSwap::from_pointee(client)),
            substrate_urls: urls,
            reconnect_lock: Arc::new(Mutex::new(())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            cache,
        })
    }

    async fn reconnect_singleflight(&self) -> Result<()> {
        // Ensure only one task performs reconnect at a time
        let _guard = self.reconnect_lock.lock().await;
        // Build a new client outside of any other locks
        let new_client = connect_with_timeouts(&self.substrate_urls).await?;
        // Atomically swap client for all readers
        self.client.store(Arc::new(new_client));
        Ok(())
    }

    pub async fn update_twin(
        &self,
        kp: &KeyPair,
        relay: RelayDomains,
        pk: Option<&[u8]>,
    ) -> Result<Hash> {
        let relay_str = Some(relay.to_string());
        // Attempt with retry on RPC disconnects
        loop {
            let client_arc = self.client.load_full();
            match client_arc.update_twin(kp, relay_str.clone(), pk).await {
                Ok(hash) => return Ok(hash),
                Err(ClientError::Rpc(_)) => {
                    // Perform singleflight reconnect and retry
                    self.reconnect_singleflight().await?;
                    continue;
                }
                Err(err) => {
                    log::warn!("update_twin non-RPC error (not retried): {}", err);
                    return Err(err.into());
                }
            }
        }
    }
}

impl<C> TwinDB for SubstrateTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    async fn get_twin(&self, twin_id: TwinID) -> Result<Option<Twin>> {
        // we can hit the cache as fast as we can here
        if let Some(twin) = self.cache.get(twin_id).await? {
            return Ok(Some(twin));
        }

        let key: u32 = twin_id.into();

        // De-duplicate concurrent cache misses for the same key
        let fetch_fut = {
            let mut map = self.in_flight.lock().await;
            if let Some(shared) = map.get(&key) {
                shared.clone()
            } else {
                let client_swap = self.client.clone();
                let urls = self.substrate_urls.clone();
                let rlock = self.reconnect_lock.clone();
                // Future that performs RPC with reconnect on RPC errors
                let fut: BoxFuture<'static, TwinFetchResult> = async move {
                    loop {
                        let client_arc = client_swap.load_full();
                        match client_arc.get_twin_by_id(key).await {
                            Ok(twin) => break Ok(Arc::new(twin.map(Twin::from))),
                            Err(ClientError::Rpc(_)) => {
                                // singleflight reconnect: take the lock, connect, swap
                                let _g = rlock.lock().await;
                                // another task may have already swapped; still safe
                                match connect_with_timeouts(&urls).await {
                                    Ok(new_client) => {
                                        client_swap.store(Arc::new(new_client));
                                    }
                                    Err(e) => {
                                        // propagate error; callers may retry later
                                        return Err(Arc::new(e));
                                    }
                                }
                                // retry
                                continue;
                            }
                            Err(err) => {
                                log::warn!(
                                    "get_twin non-RPC error for key {} (not retried): {}",
                                    key,
                                    err
                                );
                                break Err(Arc::new(err.into()));
                            }
                        }
                    }
                }
                .boxed();
                let shared = fut.shared();
                map.insert(key, shared.clone());
                shared
            }
        };

        let result = fetch_fut.await;

        // Remove from in-flight map
        let mut map = self.in_flight.lock().await;
        map.remove(&key);
        drop(map);

        let twin = match result {
            Ok(arc_opt) => (*arc_opt).clone(),
            Err(e) => return Err(anyhow!(e.as_ref().to_string())),
        };

        if let Some(ref twin) = twin {
            self.cache.set(twin.id, twin.clone()).await?;
        }
        Ok(twin)
    }

    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>> {
        // Attempt with retry on RPC disconnects
        loop {
            let client_arc = self.client.load_full();
            match client_arc.get_twin_id_by_account(account_id.clone()).await {
                Ok(id) => return Ok(id),
                Err(ClientError::Rpc(_)) => {
                    self.reconnect_singleflight().await?;
                    continue;
                }
                Err(err) => {
                    log::warn!("get_twin_with_account non-RPC error (not retried): {}", err);
                    return Err(err.into());
                }
            }
        }
    }

    async fn set_twin(&self, twin: Twin) -> Result<()> {
        self.cache.set(twin.id, twin).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::cache::{MemCache, NoCache};

    use super::*;
    use anyhow::Context;

    #[tokio::test]
    async fn test_get_twin_with_mem_cache() {
        let mem: MemCache<Twin> = MemCache::default();

        let db = SubstrateTwinDB::new(
            vec![String::from("wss://tfchain.dev.grid.tf:443")],
            Some(mem.clone()),
        )
        .await
        .context("cannot create substrate twin db object")
        .unwrap();

        let twin = db
            .get_twin(1.into())
            .await
            .context("can't get twin from substrate")
            .unwrap()
            .unwrap();

        // NOTE: this currently checks against devnet substrate
        // as provided by the url wss://tfchain.dev.grid.tf.
        // if this environment was reset at some point. those
        // values won't match anymore.
        assert!(twin.relay.is_none());
        assert!(twin.pk.is_none());
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
            .get_twin(1.into())
            .await
            .context("can't get twin from substrate")
            .unwrap()
            .unwrap();

        // NOTE: this currently checks against devnet substrate
        // as provided by the url wss://tfchain.dev.grid.tf.
        // if this environment was reset at some point. those
        // values won't match anymore.
        assert!(twin.relay.is_none());
        assert!(twin.pk.is_none());
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
