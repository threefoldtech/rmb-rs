use std::{borrow::BorrowMut, collections::HashMap, time::Duration};

use tokio::sync::RwLock;

use super::Cache;
use crate::twin::Twin;
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;

pub struct MemCache<V> {
    mem: RwLock<HashMap<String, V>>,
}

impl<V> MemCache<V> {
    pub async fn new<P: Into<String>>(prefix: P, ttl: Duration) -> Self {
        Self {
            mem: RwLock::new(HashMap::new()),
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
