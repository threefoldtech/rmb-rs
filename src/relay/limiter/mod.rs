use core::num::NonZeroUsize;
use lru::LruCache;
use std::sync::Arc;
use tokio::sync::Mutex;
use async_trait::async_trait;

mod fixed;
pub use fixed::{FixedWindow, FixedWindowOptions};

#[async_trait]
pub trait Metrics: Send + Sync + Clone + 'static {
    type Options: Send + Sync + Clone;

    fn new(option: Self::Options) -> Self;
    async fn feed(&self, size: usize) -> bool;
}

#[derive(Clone)]
pub struct Limiter<T>
where
    T: Metrics,
{
    // TODO: lru might not be the best option here
    // because it's only "promoted" when a user connects
    // hence a twin holds a connection for a long time
    // might still get dropped out of the cache.
    // suggestion: replace this with a map
    // with a periodic check to delete any metrics
    // that didn't get any updates in a long time.
    cache: Arc<Mutex<LruCache<u32, T>>>,
    options: T::Options,
}

impl<T> Limiter<T>
where
    T: Metrics + Clone,
{
    pub fn new(cap: NonZeroUsize, options: T::Options) -> Self {
        Self {
            cache: Arc::new(Mutex::new(LruCache::new(cap))),
            options,
        }
    }

    pub async fn get(&self, twin: u32) -> T {
        let mut cache = self.cache.lock().await;
        if let Some(metrics) = cache.get(&twin) {
            return metrics.clone();
        }

        let metrics = T::new(self.options.clone());
        cache.put(twin, metrics.clone());

        metrics
    }
}

impl Default for Limiter<NoLimit> {
    fn default() -> Self {
        Limiter::new(NonZeroUsize::new(1).unwrap(), ())
    }
}

#[derive(Clone)]
pub struct NoLimit;

#[async_trait]
impl Metrics for NoLimit {
    type Options = ();

    fn new(_: ()) -> Self {
        Self
    }

    async fn feed(&self, _: usize) -> bool{
        true
    }
}
