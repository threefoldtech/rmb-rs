use super::Metrics;
use std::{sync::Arc, time::UNIX_EPOCH};
use async_trait::async_trait;
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


#[async_trait]
impl Metrics for FixedWindow {
    type Options = Arc<FixedWindowOptions>;

    fn new(options: Self::Options) -> Self {
        Self {
            options,
            inner: Arc::new(Mutex::new(Counters::default())),
        }
    }

    async fn feed(&self, size: usize) -> bool{
        if size > self.options.size {
            return false;
        }

        let now = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
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

#[cfg(test)]
mod test {
    use std::{num::NonZeroUsize, sync::Arc};

    use crate::relay::limiter::{FixedWindow, FixedWindowOptions, Limiter, Metrics};

    #[tokio::test]
    async fn test_exceed_count() {
        let limiter = Limiter::<FixedWindow>::new(
            NonZeroUsize::new(10).unwrap(),
            Arc::new(FixedWindowOptions {
                count: 10,
                size: 100,
                window: 5,
            }),
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
        let limiter = Limiter::<FixedWindow>::new(
            NonZeroUsize::new(10).unwrap(),
            Arc::new(FixedWindowOptions {
                count: 100,
                size: 10,
                window: 5,
            }),
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
