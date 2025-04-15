use super::Cache;

use anyhow::{Context, Result};
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::cmd,
    RedisConnectionManager,
};
use serde::{de::DeserializeOwned, Serialize};

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
}

impl RedisCache {
    pub fn new<P: Into<String>>(pool: Pool<RedisConnectionManager>, prefix: P) -> Self {
        Self {
            pool,
            prefix: prefix.into(),
        }
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

impl<T> Cache<T> for RedisCache
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn set<S: ToString + Send + Sync>(&self, key: S, obj: T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let obj = serde_json::to_vec(&obj).context("unable to serialize twin object for redis")?;
        cmd("HSET")
            .arg(&self.prefix)
            .arg(key.to_string())
            .arg(obj)
            .query_async::<_, ()>(&mut *conn)
            .await?;

        Ok(())
    }
    async fn get<S: ToString + Send + Sync>(&self, key: S) -> Result<Option<T>> {
        let mut conn = self.get_connection().await?;

        let ret: Option<Vec<u8>> = cmd("HGET")
            .arg(&self.prefix)
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
    async fn flush(&self) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let _: () = cmd("DEL").arg(&self.prefix).query_async(&mut *conn).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    const PREFIX: &str = "twin";

    async fn create_redis_cache() -> RedisCache {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/")
            .context("unable to create redis connection manager")
            .unwrap();
        let pool = Pool::builder()
            .build(manager)
            .await
            .context("unable to build pool or redis connection manager")
            .unwrap();

        RedisCache::new(pool, PREFIX)
    }

    #[tokio::test]
    async fn test_success_set_get_string() {
        const KEY: &str = "key";
        const VAL: &str = "val";

        let cache = create_redis_cache().await;
        cache
            .set(KEY.to_owned(), VAL.to_owned())
            .await
            .context("can not set value to cache")
            .unwrap();
        let retrieved_value: Option<String> = cache
            .get(KEY.to_owned())
            .await
            .context("can not get value from the cache")
            .unwrap();

        assert_eq!(retrieved_value, Some(VAL.to_owned()));
    }

    #[tokio::test]
    async fn test_success_set_get_struct() {
        #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
        struct DummyStruct {
            pub k: usize,
        }

        const KEY: &str = "dummy";
        const VAL: DummyStruct = DummyStruct { k: 55 };

        let cache = create_redis_cache().await;
        cache
            .set(KEY.to_owned(), VAL.to_owned())
            .await
            .context("can not set value to cache")
            .unwrap();
        let retrieved_value: Option<DummyStruct> = cache
            .get(KEY.to_owned())
            .await
            .context("can not get value from the cache")
            .unwrap();

        assert_eq!(retrieved_value, Some(VAL.to_owned()));
    }
}
