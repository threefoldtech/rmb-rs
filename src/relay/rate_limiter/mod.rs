pub mod cache;

use anyhow::Result;
use async_trait::async_trait;

pub struct Throttler<T>
where T:ThrottlerCache
{
    cache: T,
    requests_per_minute: u32,
    size_per_minute: u32,
}

pub struct Params{
    pub timestamp: u64,
    pub size: usize,
}

impl<T> Throttler<T>
where T: ThrottlerCache
{
    pub fn new(cache: T, requests_per_minute: u32, size_per_minute: u32) -> Self{
        return Self { cache, requests_per_minute, size_per_minute }
    }

    /// has_exceeded_limits caches the provided request and checks if the twin has exceeded any its limits
    pub async fn can_send_message(&self, twin_id: &u32, params: &Params) -> Result<bool>{
        self.cache.can_send_message(twin_id, &params).await
    }

    pub async fn cache_message(&self, twin_id: &u32, params: &Params) -> Result<()>{
        self.cache.cache_message(twin_id, params).await
    }
}

#[async_trait]
pub trait ThrottlerCache: Send + Sync + 'static {
    /// can_send_message caches the provided request and checks if the twin has exceeded any its limits
    async fn can_send_message(& self, twin_id: &u32, params: &Params) -> Result<bool>;

    async fn cache_message(& self, twin_id: &u32, params: &Params) -> Result<()>;
}