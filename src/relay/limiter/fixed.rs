use super::{Metrics, RateLimiter};
use async_trait::async_trait;
use core::num::NonZeroUsize;
use lru::LruCache;
use std::{sync::Arc, time::UNIX_EPOCH};
use tokio::sync::Mutex;

pub struct FixedWindowOptions {
    pub size: usize,
    pub count: usize,
    pub window: usize, // window in seconds
}

#[derive(Default)]
struct Counters {
    start: u64,
    size: usize,
    count: usize,
}

#[derive(Clone)]
pub struct FixedWindow {
    options: Arc<FixedWindowOptions>,
    inner: Arc<Mutex<Counters>>,
}

impl FixedWindow {
    fn new(options: Arc<FixedWindowOptions>) -> Self {
        Self {
            options,
            inner: Arc::new(Mutex::new(Counters::default())),
        }
    }
}

#[async_trait]
impl Metrics for FixedWindow {
    async fn feed(&self, size: usize) -> bool {
        if size > self.options.size {
            return false;
        }

        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let current_window_start = now / (self.options.window as u64);
        let mut counters = self.inner.lock().await;
        if current_window_start != counters.start {
            counters.start = current_window_start;
            counters.count = 0;
            counters.size = 0;
        }
        counters.count += 1;
        counters.size += size;

        if counters.count > self.options.count || counters.size > self.options.size {
            return false;
        }

        true
    }
}

#[derive(Clone)]
pub struct FixedWindowLimiter {
    // TODO: lru might not be the best option here
    // because it's only "promoted" when a user connects
    // hence a twin holds a connection for a long time
    // might still get dropped out of the cache.
    // suggestion: replace this with a map
    // with a periodic check to delete any metrics
    // that didn't get any updates in a long time.
    cache: Arc<Mutex<LruCache<u32, FixedWindow>>>,
    options: Arc<FixedWindowOptions>,
}

impl FixedWindowLimiter {
    pub fn new(cap: NonZeroUsize, options: FixedWindowOptions) -> Self {
        Self {
            cache: Arc::new(Mutex::new(LruCache::new(cap))),
            options: Arc::new(options),
        }
    }
}

#[async_trait]
impl RateLimiter for FixedWindowLimiter {
    type Metrics = FixedWindow;

    async fn get(&self, twin: u32) -> Self::Metrics {
        let mut cache = self.cache.lock().await;
        if let Some(metrics) = cache.get(&twin) {
            return metrics.clone();
        }

        let metrics = FixedWindow::new(Arc::clone(&self.options));
        cache.push(twin, metrics.clone());
        metrics
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use super::{FixedWindowLimiter, FixedWindowOptions, Metrics};
    use crate::relay::limiter::RateLimiter;

    #[tokio::test]
    async fn test_exceed_count() {
        let limiter = FixedWindowLimiter::new(
            NonZeroUsize::new(10).unwrap(),
            FixedWindowOptions {
                count: 10,
                size: 100,
                window: 5,
            },
        );
        let twin_cache = limiter.get(1).await;
        for _ in 0..10 {
            assert!(twin_cache.feed(1).await);
        }
        assert!(!twin_cache.feed(1).await);
        std::thread::sleep(std::time::Duration::from_secs(5));
        assert!(twin_cache.feed(1).await);
    }

    #[tokio::test]
    async fn test_exceed_size() {
        let limiter = FixedWindowLimiter::new(
            NonZeroUsize::new(10).unwrap(),
            FixedWindowOptions {
                count: 100,
                size: 10,
                window: 5,
            },
        );

        let twin_cache = limiter.get(1).await;
        for _ in 0..10 {
            assert!(twin_cache.feed(1).await);
        }
        assert!(!twin_cache.feed(1).await);
        std::thread::sleep(std::time::Duration::from_secs(5));
        assert!(twin_cache.feed(1).await);
    }
}
