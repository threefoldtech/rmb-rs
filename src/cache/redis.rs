use crate::twin::Twin;

use super::Cache;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::{cmd, FromRedisValue, ToRedisArgs},
    RedisConnectionManager,
};
use serde::{Deserialize, Serialize, Serializer};

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

    async fn get_connection(&self) -> Result<PooledConnection<'_, RedisConnectionManager>> {
        let conn = self
            .pool
            .get()
            .await
            .context("unable to retrive a redis connection from the pool")?;

        Ok(conn)
    }
}

#[async_trait]
impl<'a, T> Cache<T> for RedisCache
where
    T: Serialize + Deserialize<'a> + Send + Sync + 'static,
{
    async fn set<S: ToString + Send + Sync>(&self, key: S, obj: T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let obj =
            serde_json::to_string(&obj).context("unable to serialze twin object for redis")?;
        cmd("SET")
            .arg(key.to_string())
            .arg(obj)
            .query_async(&mut *conn)
            .await?;

        Ok(())
    }
    async fn get<S: ToString + Send + Sync>(&self, key: S) -> Result<Option<T>> {
        let mut conn = self.get_connection().await?;

        let ret: Option<String> = cmd("GET")
            .arg(key.to_string())
            .query_async(&mut *conn)
            .await?;

        match ret {
            Some(val) => {
                let ret: T = serde_json::from_str(&val)
                    .context("unable to deserialze redis value to twin object")?;

                Ok(Some(ret))
            }
            None => Ok(None),
        }
    }
}
