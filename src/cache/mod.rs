mod redis;
pub use redis::RedisCache;

use async_trait::async_trait;

#[async_trait]
pub trait Cache<T>: Send + Sync {
    async fn cache(obj: T);
}
