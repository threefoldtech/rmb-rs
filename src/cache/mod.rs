mod redis;
pub use redis::RedisCache;

use anyhow::Result;
use async_trait::async_trait;
use std::marker::{Send, Sync};

#[async_trait]
pub trait Cache<T>: Send + Sync {
    async fn set<S: ToString + Send + Sync>(&self, id: S, obj: T) -> Result<()>;
    async fn get<S: ToString + Send + Sync>(&self, id: S) -> Result<Option<T>>;
}
