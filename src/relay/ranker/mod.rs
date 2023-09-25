use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::RwLock;

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
        self.failure_times.retain(|t| t.elapsed() < retain);
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
            if failure_time.elapsed() < period {
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
    relay_stats: Arc<RwLock<HashMap<String, RelayStats>>>,
    max_duration: Duration,
}

impl RelayRanker {
    pub fn new(retain: Duration) -> RelayRanker {
        RelayRanker {
            relay_stats: Arc::new(RwLock::new(HashMap::new())),
            max_duration: retain,
        }
    }

    /// report a service failure to the ranker
    pub async fn downvote(&self, domain: impl Into<String>) {
        let mut map = self.relay_stats.write().await;

        let stats = map.entry(domain.into()).or_insert(RelayStats::new());
        stats.add_failure(self.max_duration);
    }

    /// Sort the domains of relays in ascending order based on their recent failure rate.
    ///
    /// The ranking of relays is determined by the number of failures that occur during a specified period of time.
    /// This ensures that the affected relay’s rank will improve over time, and messages will be routed to it again if the service recovers.
    /// If multiple relays have the same failure rate, their order will be randomized
    pub async fn reorder(&self, domains: &mut [&str]) {
        let map = self.relay_stats.read().await;
        domains.sort_by(|a, b| {
            let a_failure_rate = map
                .get(*a)
                .map(|v| v.mean_failure_rate(self.max_duration))
                .unwrap_or(0.0);
            let b_failure_rate = map
                .get(*b)
                .map(|v| v.mean_failure_rate(self.max_duration))
                .unwrap_or(0.0);

            if a_failure_rate == b_failure_rate {
                let mut rng = rand::thread_rng();
                rng.gen::<bool>().cmp(&rng.gen::<bool>())
            } else {
                a_failure_rate.total_cmp(&b_failure_rate)
            }
        });
        log::debug!("ranking system hint: {:?}", domains);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_downvoted_at_last() {
        let ranking_system = RelayRanker::new(HOUR);

        ranking_system.downvote("bing.com").await;
        let mut domains = vec!["example.com", "bing.com", "google.com"];
        ranking_system.reorder(&mut domains).await;

        assert_eq!(*domains.last().unwrap(), "bing.com");
    }

    #[tokio::test]
    async fn test_order_by_failure_rate() {
        let ranking_system = RelayRanker::new(HOUR);

        ranking_system.downvote("bing.com").await;
        ranking_system.downvote("example.com").await;
        ranking_system.downvote("example.com").await;

        let mut domains = vec!["example.com", "bing.com", "google.com"];
        ranking_system.reorder(&mut domains).await;

        assert_eq!(domains, vec!("google.com", "bing.com", "example.com"));
    }

    #[tokio::test]
    async fn test_rank_healing() {
        let ranking_system = RelayRanker::new(HOUR);

        ranking_system.downvote("bing.com").await;

        let mut map = ranking_system.relay_stats.write().await;
        let ds = map.get_mut("bing.com").unwrap();
        if let Some(first) = ds.failure_times.get_mut(0) {
            *first = Instant::checked_sub(&Instant::now(), HOUR * 2).unwrap();
        }
        let failure_rate = ds.mean_failure_rate(HOUR);
        assert_eq!(failure_rate, 0.0);
    }
}
