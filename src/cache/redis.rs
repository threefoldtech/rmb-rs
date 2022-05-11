use crate::twin::Twin;

use super::Cache;

use anyhow::Result;
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use serde::{Deserialize, Serialize};

//
// how_to_init
// let manager = RedisConnectionManager::new(url)?;
// let pool = Pool::builder().build(manager).await?;
//

#[derive(Clone)]
pub struct RedisCache {
    pool: Pool<RedisConnectionManager>,
}

impl RedisCache {
    pub async fn new(pool: Pool<RedisConnectionManager>) -> Result<Self> {
        Ok(Self { pool })
    }
}

#[async_trait]
impl<'a, T> Cache<T> for RedisCache
where
    T: Serialize + Deserialize<'a> + Send + Sync + 'static,
{
    async fn set<S: ToString + Send + Sync>(&self, id: S, obj: T) -> Result<()> {
        Ok(())
    }
    async fn get<S: ToString + Send + Sync>(&self, id: S) -> Result<Option<T>> {
        Ok(None)
    }
}
