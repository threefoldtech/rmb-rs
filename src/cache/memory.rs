use std::sync::Arc;

use std::time::Duration;
use tokio::sync::RwLock;

#[cfg(feature = "tracker")]
use super::RMB_CACHE_HITS_TOTAL;
use super::{Cache, RMB_CACHE_ENTRIES, RMB_CACHE_FLUSHES_TOTAL, RMB_CACHE_MISSES_TOTAL};
use anyhow::Result;
use ttl_cache::TtlCache;

static IN_MEMORY_CAP: usize = 500;
static IN_MEMORY_TTL_SEC: u64 = 5 * 60;

#[derive(Clone)]
pub struct MemCache<V> {
    mem: Arc<RwLock<TtlCache<String, V>>>,
    ttl: Duration,
}

impl<V> Default for MemCache<V> {
    fn default() -> Self {
        Self::new(IN_MEMORY_CAP, Duration::from_secs(IN_MEMORY_TTL_SEC))
    }
}

impl<V> MemCache<V> {
    #[allow(unused)]
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            mem: Arc::new(RwLock::new(TtlCache::new(capacity))),
            ttl,
        }
    }
}

impl<T> Cache<T> for MemCache<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn set<K: ToString + Send + Sync>(&self, key: K, obj: T) -> Result<()> {
        let mut mem = self.mem.write().await;
        let prev = mem.insert(key.to_string(), obj, self.ttl);
        if prev.is_none() {
            RMB_CACHE_ENTRIES.inc();
        }

        Ok(())
    }

    async fn get<K: ToString + Send + Sync>(&self, key: K) -> Result<Option<T>> {
        let mem = self.mem.read().await;
        match mem.get(&key.to_string()) {
            None => {
                RMB_CACHE_MISSES_TOTAL.inc();
                Ok(None)
            }
            Some(v) => {
                #[cfg(feature = "tracker")]
                {
                    RMB_CACHE_HITS_TOTAL.inc();
                }
                Ok(Some(v.clone()))
            }
        }
    }
    async fn flush(&self) -> Result<()> {
        let mut mem = self.mem.write().await;
        mem.clear();
        RMB_CACHE_ENTRIES.set(0);
        RMB_CACHE_FLUSHES_TOTAL.inc();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[tokio::test]
    async fn test_success_set_get_string() {
        let cache = MemCache::default();
        cache
            .set("k".to_string(), "v".to_string())
            .await
            .context("can not set value to cache")
            .unwrap();
        let retrieved_value: Option<String> = cache
            .get("k")
            .await
            .context("can not get value from the cache")
            .unwrap();

        assert_eq!(retrieved_value, Some("v".to_string()));
    }

    #[tokio::test]
    async fn test_success_set_get_struct() {
        #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
        struct DummyStruct {
            pub k: String,
        }

        let some_val = DummyStruct { k: "v".to_string() };

        let cache = MemCache::default();

        cache
            .set("k".to_string(), some_val.clone())
            .await
            .context("can not set value to cache")
            .unwrap();

        let retrieved_value: Option<DummyStruct> = cache
            .get("k")
            .await
            .context("can not get value from the cache")
            .unwrap();

        assert_eq!(retrieved_value, Some(some_val));
    }
}
