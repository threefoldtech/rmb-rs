use async_trait::async_trait;
use core::num::NonZeroUsize;

mod fixed;
use fixed::{FixedWindow, FixedWindowLimiter};

pub use fixed::FixedWindowOptions;

#[async_trait]
pub trait Metrics: Send + Sync + Clone + 'static {
    async fn feed(&self, size: usize) -> bool;
}

#[async_trait]
pub trait RateLimiter: Send + Sync + Clone + 'static {
    type Metrics: Metrics;

    async fn get(&self, twin: u32) -> Self::Metrics;
}

#[derive(Clone)]
pub enum Limiters {
    NoLimit,
    FixedWindow(FixedWindowLimiter),
}

impl Limiters {
    pub fn no_limit() -> Self {
        Self::NoLimit
    }

    pub fn fixed_window(cap: NonZeroUsize, options: FixedWindowOptions) -> Self {
        Self::FixedWindow(FixedWindowLimiter::new(cap, options))
    }
}

#[derive(Clone)]
pub enum LimitersMetrics {
    NoLimit,
    FixedWindow(FixedWindow),
}

#[async_trait]
impl Metrics for LimitersMetrics {
    async fn feed(&self, size: usize) -> bool {
        match self {
            Self::NoLimit => true,
            Self::FixedWindow(ref f) => f.feed(size).await,
        }
    }
}

#[async_trait]
impl RateLimiter for Limiters {
    type Metrics = LimitersMetrics;
    async fn get(&self, twin: u32) -> Self::Metrics {
        match self {
            Self::NoLimit => LimitersMetrics::NoLimit,
            Self::FixedWindow(ref limiter) => LimitersMetrics::FixedWindow(limiter.get(twin).await),
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
            Limiters::no_limit()
        } else {
            Limiters::fixed_window(
                count,
                FixedWindowOptions {
                    count: 1000,
                    size: 1000,
                    window: 60,
                },
            )
        };
    }
}
