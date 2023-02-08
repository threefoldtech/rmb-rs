pub mod lfu_cache;

use anyhow::Result;
use async_trait::async_trait;

pub struct Throttler<T>
where
    T: ThrottlerCache,
{
    cache: T,
}

#[derive(Debug)]
pub struct Params {
    pub timestamp: u64,
    pub size: usize,
}

impl<T> Throttler<T>
where
    T: ThrottlerCache,
{
    pub fn new(cache: T) -> Self {
        Self { cache }
    }

    pub async fn can_send_message(&self, twin_id: &u32, params: &Params) -> Result<bool> {
        self.cache.can_send_message(twin_id, params).await
    }

    pub async fn cache_message(&self, twin_id: &u32, params: &Params) -> Result<()> {
        self.cache.cache_message(twin_id, params).await
    }
}

#[async_trait]
pub trait ThrottlerCache: Send + Sync + 'static {
    /// can_send_message checks if this twin can send a new massage with the provided paramters
    async fn can_send_message(&self, twin_id: &u32, params: &Params) -> Result<bool>;

    /// cache_message caches the provided paramters of the message
    async fn cache_message(&self, twin_id: &u32, params: &Params) -> Result<()>;
}
