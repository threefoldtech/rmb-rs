mod redis;
pub use redis::RedisCache;

use std::marker::{Send, Sync};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Cache<T>: Send + Sync {
    async fn set<S: ToString + Send + Sync>(id: S, obj: T) -> Result<()>;
    async fn get<S: ToString + Send + Sync>(id: S) -> Result<Option<T>>;
}
