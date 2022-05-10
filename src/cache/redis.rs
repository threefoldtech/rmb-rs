use crate::twin::Twin;

use super::Cache;

use anyhow::Result;
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};

pub struct RedisCache {
    pool: Pool<RedisConnectionManager>,
}

impl RedisCache {
    pub async fn new(url: String) -> Result<Self> {
        let manager = RedisConnectionManager::new(url)?;
        let pool = Pool::builder().build(manager).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Cache<Twin> for RedisCache {
    async fn cache(obj: Twin) {
        todo!()
    }
}
