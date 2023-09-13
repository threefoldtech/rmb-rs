use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::Rng;

pub const HOUR: Duration = Duration::from_secs(3600);
#[derive(Debug, Clone)]
struct RelayStats {
    pub failure_times: Vec<Instant>,
}

impl RelayStats {
    fn new() -> RelayStats {
        RelayStats {
            failure_times: Vec::new(),
        }
    }

    /// Keep only the failures that happened during the specified recent period.
    fn _clean(&mut self, retain: Duration) {
        let count = self.failure_times.len();
        self.failure_times.retain(|t| {
            t.elapsed() < retain
        });
        log::trace!("cleaning {:?} entires", count - self.failure_times.len());
    }

    fn add_failure(&mut self, retain: Duration) {
        self.failure_times.push(Instant::now());
        self._clean(retain);
    }

    /// Return the count of the failures that happened during the specified recent period.
    fn failures_last(&self, period: Duration) -> usize {
        let mut count = 0;
        for failure_time in &self.failure_times {
            if failure_time
                .elapsed() < period
            {
                break;
            }
            count += 1;
        }
        self.failure_times.len() - count
    }

    /// Return the mean failure rate per hour based on the known failures happened during the specified recent period.
    fn mean_failure_rate(&self, period: Duration) -> f64 {
        let failures = self.failures_last(period);

        if failures == 0 {
            return 0.0;
        }
        failures as f64 / (period.as_secs_f64() / 3600.0)
    }
}

#[derive(Debug, Clone)]
pub struct RelayRanker {
    relay_stats: Arc<Mutex<RefCell<HashMap<String, RelayStats>>>>,
    max_duration: Duration,
}

impl RelayRanker {
    pub fn new(retain: Duration) -> RelayRanker {
        RelayRanker {
            relay_stats: Arc::new(Mutex::new(RefCell::new(HashMap::new()))),
            max_duration: retain,
        }
    }

    /// report a service failure to the ranker
    pub fn downvote(&self, domain: impl Into<String>) -> Result<()> {
        let guard = match self.relay_stats.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(anyhow::anyhow!("failed to acquire lock")),
        };
        let mut inner = guard.borrow_mut();
        let stats = inner.entry(domain.into()).or_insert(RelayStats::new());
        stats.add_failure(self.max_duration);
        Ok(())
    }

    /// Sort the domains of relays in ascending order based on their recent failure rate.
    ///
    /// The ranking of relays is determined by the number of failures that occur during a specified period of time.
    /// This ensures that the affected relay’s rank will improve over time, and messages will be routed to it again if the service recovers.
    /// If multiple relays have the same failure rate, their order will be randomized
    pub fn reorder(&self, domains: &mut Vec<&str>) -> Result<()> {
        let guard = match self.relay_stats.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(anyhow::anyhow!("failed to acquire lock")),
        };

        domains.sort_by(|a, b| {
            let a_failure_rate = self._mean_failure_rate(*a, &guard);
            let b_failure_rate = self._mean_failure_rate(*b, &guard);
            if a_failure_rate == b_failure_rate {
                let mut rng = rand::thread_rng();
                rng.gen::<bool>().cmp(&rng.gen::<bool>())
            } else {
                a_failure_rate
                    .total_cmp(&b_failure_rate)
            }
        });
        log::debug!("ranking system hint: {:?}", domains);
        Ok(())
    }

    fn _mean_failure_rate(
        &self,
        domain: impl Into<String>,
        guard: &MutexGuard<'_, RefCell<HashMap<String, RelayStats>>>,
    ) -> f64 {
        let inner = guard.borrow();
        if let Some(stats) = inner.get(&domain.into()) {
            stats.mean_failure_rate(self.max_duration)
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_downvoted_at_last() {
        let ranking_system = RelayRanker::new(HOUR);

        let _ = ranking_system.downvote("bing.com");
        let mut domains = vec!["example.com", "bing.com", "google.com"];
        let _ = ranking_system.reorder(&mut domains);

        assert_eq!(*domains.last().unwrap(), "bing.com");
    }

    #[test]
    fn test_order_by_failure_rate() {
        let ranking_system = RelayRanker::new(HOUR);

        let _ = ranking_system.downvote("bing.com");
        let _ = ranking_system.downvote("example.com");
        let _ = ranking_system.downvote("example.com");

        let mut domains = vec!["example.com", "bing.com", "google.com"];
        let _ = ranking_system.reorder(&mut domains);

        assert_eq!(domains, vec!("google.com", "bing.com", "example.com"));
    }

    #[test]
    fn test_rank_healing() {
        let ranking_system = RelayRanker::new(HOUR);

        let _ = ranking_system.downvote("bing.com");

        let binding = ranking_system.relay_stats.lock().unwrap();
        let mut inner = binding.borrow_mut();
        let ds = inner.get_mut("bing.com").unwrap();
        if let Some(first) = ds.failure_times.get_mut(0) {
            *first = Instant::checked_sub(&Instant::now(), HOUR * 2).unwrap();
        }
        let failure_rate = ds.mean_failure_rate(HOUR);
        assert_eq!(failure_rate, 0.0);
    }
}
