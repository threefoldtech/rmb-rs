mod memory;
mod redis;
pub use memory::MemCache;
pub use redis::RedisCache;

use anyhow::Result;
use std::{
    future::Future,
    marker::{Send, Sync},
};

use lazy_static::lazy_static;
use prometheus::{IntCounter, IntGauge, Registry};

#[cfg(feature = "tracker")]
lazy_static! {
    static ref RMB_CACHE_HITS_TOTAL: IntCounter = IntCounter::new(
        "rmb_cache_hits_total",
        "Number of cache hits when fetching twins",
    )
    .unwrap();
}

lazy_static! {
    static ref RMB_CACHE_MISSES_TOTAL: IntCounter = IntCounter::new(
        "rmb_cache_misses_total",
        "Number of cache misses when fetching twins (RPC fallback likely)",
    )
    .unwrap();
    static ref RMB_CACHE_FLUSHES_TOTAL: IntCounter = IntCounter::new(
        "rmb_cache_flushes_total",
        "Number of times the twin cache has been flushed",
    )
    .unwrap();
    static ref RMB_CACHE_ENTRIES: IntGauge = IntGauge::new(
        "rmb_cache_entries",
        "Current number of entries in the twin cache",
    )
    .unwrap();
}

pub fn register_cache_metrics(registry: &Registry) {
    // Best-effort register into provided registry; ignore duplicate registration errors.
    #[cfg(feature = "tracker")]
    let _ = registry.register(Box::new(RMB_CACHE_HITS_TOTAL.clone()));
    let _ = registry.register(Box::new(RMB_CACHE_MISSES_TOTAL.clone()));
    let _ = registry.register(Box::new(RMB_CACHE_FLUSHES_TOTAL.clone()));
    let _ = registry.register(Box::new(RMB_CACHE_ENTRIES.clone()));
}

pub trait Cache<T>: Send + Sync + 'static {
    fn set<S: ToString + Send + Sync>(
        &self,
        id: S,
        obj: T,
    ) -> impl Future<Output = Result<()>> + Send;
    fn get<S: ToString + Send + Sync>(
        &self,
        id: S,
    ) -> impl Future<Output = Result<Option<T>>> + Send;
    fn flush(&self) -> impl Future<Output = Result<()>> + Send;
}

impl<T, C> Cache<T> for Option<C>
where
    C: Cache<T>,
    T: Send + Sync + 'static,
{
    async fn set<S: ToString + Send + Sync>(&self, id: S, obj: T) -> Result<()> {
        match self {
            Some(cache) => cache.set(id, obj).await,
            None => Ok(()),
        }
    }
    async fn get<S: ToString + Send + Sync>(&self, id: S) -> Result<Option<T>> {
        match self {
            Some(cache) => cache.get(id).await,
            None => Ok(None),
        }
    }
    async fn flush(&self) -> Result<()> {
        match self {
            Some(cache) => cache.flush().await,
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy)]
pub struct NoCache;

impl<T> Cache<T> for NoCache
where
    T: Send + Sync + 'static,
{
    async fn set<S: ToString + Send + Sync>(&self, _id: S, _obj: T) -> Result<()> {
        Ok(())
    }
    async fn get<S: ToString + Send + Sync>(&self, _id: S) -> Result<Option<T>> {
        Ok(None)
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
