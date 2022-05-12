use std::time::Duration;

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
pub struct RedisCache {
    pool: Pool<RedisConnectionManager>,
    prefix: String,
    ttl: Duration,
}

impl RedisCache {
    pub fn new<P: Into<String>>(
        pool: Pool<RedisConnectionManager>,
        prefix: P,
        ttl: Duration,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            prefix: prefix.into(),
            ttl,
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
impl<T> Cache<T> for RedisCache
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn set<S: ToString + Send + Sync>(&self, key: S, obj: T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let obj = serde_json::to_vec(&obj).context("unable to serialize twin object for redis")?;
        let key = format!("{}.{}", self.prefix, key.to_string());
        cmd("SET")
            .arg(key)
            .arg(obj)
            .arg("EX")
            .arg(self.ttl.as_secs())
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

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    async fn create_redis_cache() -> RedisCache {
        let manager = RedisConnectionManager::new("url")
            .context("unable to create redis connection manager")
            .unwrap();
        let pool = Pool::builder()
            .build(manager)
            .await
            .context("unable to build pool or redis connection manager")
            .unwrap();
        let cache = RedisCache::new(pool, "twin", Duration::from_secs(20))
            .context("unable to create redis cache")
            .unwrap();

        cache
    }

    #[tokio::test]
    async fn test_success_set_get_string() {
        let cache = create_redis_cache().await;
        cache
            .set("k".to_string(), "v".to_string())
            .await
            .context("can not set value to cache")
            .unwrap();
        let retrieved_value: Option<String> = cache
            .get("k")
            .await
            .context("can not get value from the cache")
            .unwrap();

        assert_eq!(retrieved_value, Some("v".to_string()));
    }

    #[tokio::test]
    async fn test_success_set_get_struct() {
        #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
        struct DummyStruct {
            pub k: String,
        }

        let some_val = DummyStruct { k: "v".to_string() };

        let cache = create_redis_cache().await;

        cache
            .set("k".to_string(), some_val.clone())
            .await
            .context("can not set value to cache")
            .unwrap();

        let retrieved_value: Option<DummyStruct> = cache
            .get("k")
            .await
            .context("can not get value from the cache")
            .unwrap();

        assert_eq!(retrieved_value, Some(some_val));
    }
}
