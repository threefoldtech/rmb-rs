use std::{
    borrow::BorrowMut,
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use super::Cache;
use crate::twin::Twin;
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;

pub struct MemCache<V> {
    mem: Mutex<HashMap<String, V>>,
    prefix: String,
    ttl: Duration,
}

impl<V> MemCache<V> {
    pub async fn new<P: Into<String>>(prefix: P, ttl: Duration) -> Self {
        Self {
            mem: Mutex::new(HashMap::new()),
            prefix: prefix.into(),
            ttl,
        }
    }
}

#[async_trait]
impl<T> Cache<T> for MemCache<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn set<K: ToString + Send + Sync>(&self, key: K, obj: T) -> Result<()> {
        let mut mem = self
            .mem
            .lock()
            .map_err(|err| anyhow!("{}", err))
            .context("cannot acquire the in-memory cache")?;

        mem.insert(key.to_string(), obj);
        Ok(())
    }

    async fn get<K: ToString + Send + Sync>(&self, key: K) -> Result<Option<T>> {
        let mem = self
            .mem
            .lock()
            .map_err(|err| anyhow!("{}", err))
            .context("cannot acquire the in-memory cache")?;

        match mem.get(&key.to_string()) {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }
}
