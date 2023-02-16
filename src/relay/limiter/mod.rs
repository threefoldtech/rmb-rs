use async_trait::async_trait;
use core::num::NonZeroUsize;
use lru::LruCache;
use std::sync::Arc;
use tokio::sync::Mutex;

mod fixed;
pub use fixed::{FixedWindow, FixedWindowOptions};

#[async_trait]
pub trait Metrics: Send + Sync + Clone + 'static {
    type Options: Send + Sync + Clone;

    fn new(option: Self::Options) -> Self;
    async fn feed(&self, size: usize) -> bool;
}

#[async_trait]
pub trait RateLimiter: Send + Sync + Clone + 'static {
    type Feeder: Metrics;
    async fn get_metrics(&self, twin: u32) -> Self::Feeder;
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
}

#[async_trait]
impl<T> RateLimiter for Limiter<T>
where
    T: Metrics + Clone,
{
    type Feeder = T;
    async fn get_metrics(&self, twin: u32) -> Self::Feeder {
        let mut cache = self.cache.lock().await;
        if let Some(metrics) = cache.get(&twin) {
            return metrics.clone();
        }

        let metrics = T::new(self.options.clone());
        cache.push(twin, metrics.clone());
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
impl RateLimiter for NoLimit {
    type Feeder = NoLimit;
    async fn get_metrics(&self, _: u32) -> Self::Feeder {
        NoLimit {}
    }
}

#[async_trait]
impl Metrics for NoLimit {
    type Options = ();

    fn new(_: ()) -> Self {
        Self
    }

    async fn feed(&self, _: usize) -> bool {
        true
    }
}

#[derive(Clone)]
pub enum LimitersOptions {
    NoLimit,
    FixedWindow(Arc<FixedWindowOptions>),
}

impl LimitersOptions {
    pub fn no_limit() -> Self {
        Self::NoLimit
    }

    pub fn fixed_window(fixed: FixedWindowOptions) -> Self {
        Self::FixedWindow(Arc::new(fixed))
    }
}

#[derive(Clone)]
pub enum Limiters {
    NoLimit,
    FixedWindow(FixedWindow),
}

#[async_trait]
impl Metrics for Limiters {
    type Options = LimitersOptions;

    fn new(options: Self::Options) -> Self {
        match options {
            LimitersOptions::NoLimit => Self::NoLimit,
            LimitersOptions::FixedWindow(options) => Self::FixedWindow(FixedWindow::new(options)),
        }
    }

    async fn feed(&self, size: usize) -> bool {
        match self {
            Self::NoLimit => true,
            Self::FixedWindow(ref window) => window.feed(size).await,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use core::num::NonZeroUsize;

    #[test]
    fn static_dispatch() {
        let count = NonZeroUsize::new(10).unwrap();
        let _ = if true {
            Limiter::<Limiters>::new(count, LimitersOptions::no_limit())
        } else {
            Limiter::<Limiters>::new(
                count,
                LimitersOptions::fixed_window(FixedWindowOptions {
                    count: 1000,
                    size: 1000,
                    window: 60,
                }),
            )
        };
    }
}
