use super::{Metrics, RateLimiter};
use async_trait::async_trait;
use core::num::NonZeroUsize;
use lru::LruCache;
use std::{sync::Arc, time::UNIX_EPOCH};
use tokio::sync::Mutex;

/// FixedWindowOptions are used to determine how many messages a twin is allowed to send in a time window, this is assigned to `count`, 
/// and the total size of the messages in bytes a twin is allowed to send in a time window, this is assigned to `size`, 
/// and the size of that window in seconds, this is assigned to `window`.
pub struct FixedWindowOptions {
    pub size: usize,
    pub count: usize,
    pub window: usize, // window in seconds
}

/// Counters are used to determine the usage (`size` and `count` of messages) of some user during a time window started at a timestamp equal to `start`.
#[derive(Default)]
struct Counters {
    start: u64,
    size: usize,
    count: usize,
}

/// FixedWindow is a `Metrics` implementation. Using FixedWindow metrics, a user could only have a fixed rate of messages at each time window.
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
    /// feed returns `true` if a user hasn't exceeded his consumption limits, `false` otherwise.
    /// if the current timestamp is different from the current counter start, the counter is reset to zero values, with the start equal to the current timestamp.
    /// then, after incrementing the counters, the limits are checked, and a boolean value is returned accordingly.
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

/// FixedWindowLimiter is a `RateLimiter` implementation. it uses `FixedWindow` metrics to evaluate whether a user exceeded their limits or not, and an LRU cache to store twins' metrics.
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
    /// get returns the corresponding metrics for the specified twin. if no metrics are stored for this twin, a new one is created.
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
