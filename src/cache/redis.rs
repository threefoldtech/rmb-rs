use crate::twin::Twin;

use super::Cache;

use anyhow::{Result, Ok};
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};

//
// how_to_init
// let manager = RedisConnectionManager::new(url)?;
// let pool = Pool::builder().build(manager).await?;
//
pub struct RedisCache {
    pool: Pool<RedisConnectionManager>,
}

impl RedisCache {
    pub async fn new(pool: Pool<RedisConnectionManager>) -> Result<Self> {
        Ok(Self { pool })
    }
}

#[async_trait]
impl Cache<Twin> for RedisCache {
    async fn set<S: ToString + Send + Sync>(id: S, obj: Twin) -> Result<()> {
        
        Ok(())
    }
    async fn get<S: ToString + Send + Sync>(id: S) -> Result<Option<&'async_trait Twin>> {

        Ok(None)
    }
}
