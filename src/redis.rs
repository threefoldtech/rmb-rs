use anyhow::{Context, Result};
use bb8_redis::{
    bb8::{Pool, PooledConnection},
    redis::{cmd, FromRedisValue, ToRedisArgs},
    RedisConnectionManager,
};

pub async fn pool<S: AsRef<str>>(url: S) -> Result<Pool<RedisConnectionManager>> {
    let mgr = RedisConnectionManager::new(url.as_ref())?;
    Ok(Pool::builder().max_size(20).build(mgr).await?)
}
