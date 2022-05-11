use crate::twin::Twin;

use super::Cache;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::{cmd, FromRedisValue, ToRedisArgs},
    RedisConnectionManager,
};
use serde::{de::DeserializeOwned, Serialize, Serializer};

//
// how_to_init
// let manager = RedisConnectionManager::new(url)?;
// let pool = Pool::builder().build(manager).await?;
// RedisCache::new(pool, "twin", 10); //10 seconds
//

#[derive(Clone)]
pub struct RedisCache<P>
where
    P: Into<String>,
{
    pool: Pool<RedisConnectionManager>,
    prefix: P,
    ttl: usize,
}

impl<P> RedisCache<P>
where
    P: Into<String>,
{
    pub async fn new(
        pool: Pool<RedisConnectionManager>,
        prefix: P,
        ttl_sec: usize,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            prefix,
            ttl: ttl_sec,
        })
    }

    async fn get_connection(&self) -> Result<PooledConnection<'_, RedisConnectionManager>> {
        let conn = self
            .pool
            .get()
            .await
            .context("unable to retrieve a redis connection from the pool")?;

        Ok(conn)
    }
}

#[async_trait]
impl<T, P> Cache<T> for RedisCache<P>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    P: Into<String> + Send + Sync + Clone,
{
    async fn set<S: ToString + Send + Sync>(&self, key: S, obj: T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let obj = serde_json::to_vec(&obj).context("unable to serialize twin object for redis")?;
        let mut key = format!("{:?}.{}", self.prefix.clone().into(), key.to_string());
        cmd("SET")
            .arg(key)
            .arg(obj)
            .arg("EX")
            .arg(self.ttl)
            .query_async(&mut *conn)
            .await?;

        Ok(())
    }
    async fn get<S: ToString + Send + Sync>(&self, key: S) -> Result<Option<T>> {
        let mut conn = self.get_connection().await?;

        let ret: Option<Vec<u8>> = cmd("GET")
            .arg(key.to_string())
            .query_async(&mut *conn)
            .await?;

        match ret {
            Some(val) => {
                let ret: T = serde_json::from_slice(&val)
                    .context("unable to deserialize redis value to twin object")?;

                Ok(Some(ret))
            }
            None => Ok(None),
        }
    }
}
