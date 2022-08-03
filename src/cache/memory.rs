use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use super::Cache;
use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone)]
pub struct MemCache<V> {
    mem: Arc<RwLock<HashMap<String, V>>>,
}

impl<V> Default for MemCache<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> MemCache<V> {
    #[allow(unused)]
    pub fn new() -> Self {
        Self {
            mem: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl<T> Cache<T> for MemCache<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn set<K: ToString + Send + Sync>(&self, key: K, obj: T) -> Result<()> {
        let mut mem = self.mem.write().await;
        mem.insert(key.to_string(), obj);

        Ok(())
    }

    async fn get<K: ToString + Send + Sync>(&self, key: K) -> Result<Option<T>> {
        let mem = self.mem.read().await;
        match mem.get(&key.to_string()) {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[tokio::test]
    async fn test_success_set_get_string() {
        let cache = MemCache::new();
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

        let cache = MemCache::new();

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
