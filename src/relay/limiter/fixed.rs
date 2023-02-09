use super::Metrics;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct FixedWindowOptions {
    pub size: usize,
    pub count: usize,
    pub window: usize,
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

impl Metrics for FixedWindow {
    type Options = FixedWindowOptions;

    fn new(options: Self::Options) -> Self {
        Self {
            options,
            inner: Arc::new(Mutex::new(Counters::default())),
        }
    }

    fn feed(&self, size: usize) -> bool {
        false
    }
}
