mod memory;
mod redis;
pub use memory::MemCache;
pub use redis::RedisCache;

use anyhow::Result;
use async_trait::async_trait;
use std::marker::{Send, Sync};

#[async_trait]
pub trait Cache<T>: Send + Sync + 'static {
    async fn set<S: ToString + Send + Sync>(&self, id: S, obj: T) -> Result<()>;
    async fn get<S: ToString + Send + Sync>(&self, id: S) -> Result<Option<T>>;
    async fn flush(&self) -> Result<()>;
}

#[async_trait]
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

#[async_trait]
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
