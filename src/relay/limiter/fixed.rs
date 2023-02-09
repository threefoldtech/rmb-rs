use super::Metrics;
use std::{sync::Arc, time::UNIX_EPOCH};
use async_trait::async_trait;
use tokio::sync::Mutex;

#[derive(Clone)]
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
    options: FixedWindowOptions,
    inner: Arc<Mutex<Counters>>,
}


#[async_trait]
impl Metrics for FixedWindow {
    type Options = FixedWindowOptions;

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
        
        if counters.count + 1 > self.options.count ||
            counters.size + size > self.options.size {
            return false;
        }
        
        counters.count += 1;
        counters.size += size;

        true
    }
    
}