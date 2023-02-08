use super::{Params, ThrottlerCache};
use anyhow::Result;
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::{RwLock, RwLockWriteGuard};

pub struct LFUCache {
    clean_up: CleanUp,
    twins_map: Arc<RwLock<HashMap<u32, TwinCache>>>,
    frequency_map: Arc<RwLock<BTreeMap<u32, HashSet<u32>>>>,
    capacity: usize,
    time_window: u64,
    requests_per_window: usize,
    size_per_window: usize,
}

#[derive(Clone)]
pub enum CleanUp {
    SlidingWindow,
    FixedWindow,
}

#[derive(Debug)]
struct TwinCache {
    params: VecDeque<Params>,
    total_size: usize,
    frequency: u32,
}

impl Clone for LFUCache {
    fn clone(&self) -> Self {
        Self {
            clean_up: self.clean_up.clone(),
            twins_map: self.twins_map.clone(),
            frequency_map: self.frequency_map.clone(),
            capacity: self.capacity,
            time_window: self.time_window,
            requests_per_window: self.requests_per_window,
            size_per_window: self.size_per_window,
        }
    }
}

impl LFUCache {
    pub fn new(
        clean_up: CleanUp,
        capacity: usize,
        time_window: u64,
        requests_per_window: usize,
        size_per_window: usize,
    ) -> Self {
        Self {
            clean_up,
            twins_map: Arc::new(RwLock::new(HashMap::new())),
            frequency_map: Arc::new(RwLock::new(BTreeMap::new())),
            capacity,
            requests_per_window,
            size_per_window,
            time_window,
        }
    }

    /// remove_outdate_data removes twin data that is out of our tracked window
    async fn remove_outdated_data(&self, twin_id: &u32, params: &Params) -> Result<()> {
        let mut twins_map = self.twins_map.write().await;
        let twin_cache = match twins_map.get_mut(twin_id) {
            Some(cache) => cache,
            None => return Ok(()),
        };
        loop {
            let front = twin_cache.params.front();
            match front {
                Some(front) => {
                    match self.clean_up {
                        CleanUp::SlidingWindow => {
                            if params.timestamp - front.timestamp >= self.time_window {
                                let deleted = twin_cache.params.pop_front().unwrap();
                                twin_cache.total_size -= deleted.size;
                                continue;
                            }
                        }
                        CleanUp::FixedWindow => {
                            // if the incoming message's timestamp is not in the same time_window as the first cached message,
                            // then delete the first cached message.
                            if self.time_window == 0 {
                                twin_cache.params.clear();
                                twin_cache.total_size = 0;
                                break;
                            }
                            if params.timestamp / self.time_window
                                != front.timestamp / self.time_window
                            {
                                let deleted = twin_cache.params.pop_front().unwrap();
                                twin_cache.total_size -= deleted.size;
                                continue;
                            }
                        }
                    }
                    break;
                }
                None => break,
            }
        }

        Ok(())
    }
}

/// insert_new_twin inserts a new twin into cache
async fn insert_new_twin<'a>(
    twin_id: &u32,
    params: &Params,
    capacity: usize,
    twins_map: &mut RwLockWriteGuard<'a, HashMap<u32, TwinCache>>,
    frequency_map: &mut RwLockWriteGuard<'a, BTreeMap<u32, HashSet<u32>>>,
) -> Result<()> {
    // check cache capacity
    // if capacity is full, remove least frequently used twin
    // insert new twin with frequency of 1
    if capacity == twins_map.len() {
        remove_least_frequently_used(twins_map, frequency_map).await?;
    }

    if twins_map.get(twin_id).is_some() {
        return Err(anyhow!("twin {} is already stored in map", twin_id));
    }

    twins_map.insert(
        *twin_id,
        TwinCache {
            params: VecDeque::from([Params {
                size: params.size,
                timestamp: params.timestamp,
            }]),
            total_size: params.size,
            frequency: 1,
        },
    );

    if frequency_map.get(&1).is_none() {
        frequency_map.insert(1, HashSet::new());
    }

    let map = frequency_map.get_mut(&1).unwrap();
    map.insert(*twin_id);

    Ok(())
}

/// remove_least_frequently_used removes one of the least frequently used twins from cache
async fn remove_least_frequently_used<'a>(
    twins_map: &mut RwLockWriteGuard<'a, HashMap<u32, TwinCache>>,
    frequency_map: &mut RwLockWriteGuard<'a, BTreeMap<u32, HashSet<u32>>>,
) -> Result<()> {
    // first check if capacity is full, return ok if not
    let mut entry = match frequency_map.first_entry() {
        Some(entry) => entry,
        None => return Err(anyhow!("frequency map is empty")),
    };

    let twins_set = entry.get_mut();
    let random_twin = match twins_set.iter().next() {
        Some(x) => x,
        None => return Err(anyhow!("twins set is empty")),
    };
    let to_be_deleted = *random_twin;
    twins_set.remove(&to_be_deleted);
    if twins_set.is_empty() {
        let key = entry.key().to_owned();
        frequency_map.remove(&key);
    }

    twins_map.remove(&to_be_deleted);
    Ok(())
}

#[async_trait]
impl ThrottlerCache for LFUCache {
    async fn can_send_message(&self, twin_id: &u32, params: &Params) -> Result<bool> {
        // checks if message size is larger than limit
        // removes data outside time window
        // checks if number of messages sent in window is below threshold
        if params.size > self.size_per_window {
            return Ok(false);
        }

        self.remove_outdated_data(twin_id, params).await?;

        let twins_map = self.twins_map.read().await;
        let twins_cache = match twins_map.get(twin_id) {
            Some(cache) => cache,
            None => return Ok(true),
        };

        if twins_cache.params.len() == self.requests_per_window
            || twins_cache.total_size + params.size > self.size_per_window
        {
            return Ok(false);
        }

        return Ok(true);
    }

    async fn cache_message(&self, twin_id: &u32, params: &Params) -> Result<()> {
        // cache message:
        //  1- get twin old frequency
        //  2- remove twin from list in frequency map
        //  3- increase twin frequency
        //  4- add new params
        //  5- add twin to list in frequency map using new frequency
        let mut twins_map = self.twins_map.write().await;
        let mut frequency_map = self.frequency_map.write().await;

        let twin_cache = match twins_map.get_mut(twin_id) {
            Some(cache) => cache,
            None => {
                insert_new_twin(
                    twin_id,
                    params,
                    self.capacity,
                    &mut twins_map,
                    &mut frequency_map,
                )
                .await?;
                return Ok(());
            }
        };

        let old_frequency = twin_cache.frequency;
        // remove old twin from frequency map using old frequency
        let twins_set = match frequency_map.get_mut(&old_frequency) {
            Some(list) => list,
            None => return Err(anyhow!("twin {} was not found in frequency map", twin_id)),
        };

        if !twins_set.remove(twin_id) {
            return Err(anyhow!("twin {} was not found in frequency map", twin_id));
        }
        if twins_set.is_empty() {
            frequency_map.remove(&old_frequency);
        }

        let new_frequency = old_frequency + 1;

        twin_cache.params.push_back(Params {
            timestamp: params.timestamp,
            size: params.size,
        });
        twin_cache.total_size += params.size;
        twin_cache.frequency = new_frequency;

        // insert twin to frequency map with new frequency
        match frequency_map.get_mut(&new_frequency) {
            Some(list) => {
                list.insert(*twin_id);
            }
            None => {
                let mut list = HashSet::new();
                list.insert(*twin_id);
                frequency_map.insert(new_frequency, list);
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::UNIX_EPOCH;

    use crate::relay::throttler::{Params, Throttler};

    use super::{CleanUp, LFUCache};

    #[tokio::test]
    async fn test_exceed_size() {
        exceed_size(CleanUp::FixedWindow).await;
        exceed_size(CleanUp::SlidingWindow).await;
    }

    async fn exceed_size(clean_up: CleanUp) {
        let lfu = LFUCache::new(clean_up, 10, 60, 100, 10);
        let throttler = Throttler::new(lfu);
        for _ in 0..10 {
            throttler
                .cache_message(
                    &1,
                    &Params {
                        size: 1,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    },
                )
                .await
                .unwrap();
        }
        assert!(!throttler
            .can_send_message(
                &1,
                &Params {
                    size: 1,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                }
            )
            .await
            .unwrap())
    }

    #[tokio::test]
    async fn test_exceed_requests() {
        exceed_requests(CleanUp::FixedWindow).await;
        exceed_requests(CleanUp::SlidingWindow).await;
    }

    async fn exceed_requests(clean_up: CleanUp) {
        let lfu = LFUCache::new(clean_up, 10, 60, 10, 100);
        let throttler = Throttler::new(lfu);
        for _ in 0..10 {
            throttler
                .cache_message(
                    &1,
                    &Params {
                        size: 1,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    },
                )
                .await
                .unwrap();
        }
        assert!(!throttler
            .can_send_message(
                &1,
                &Params {
                    size: 1,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                }
            )
            .await
            .unwrap())
    }

    #[tokio::test]
    async fn test_exceed_capacity() {
        exceed_capacity(CleanUp::FixedWindow).await;
        exceed_capacity(CleanUp::SlidingWindow).await;
    }
    async fn exceed_capacity(clean_up: CleanUp) {
        let lfu = LFUCache::new(clean_up, 10, 60, 100, 1);
        let throttler = Throttler::new(lfu);
        let params = Params {
            size: 1,
            timestamp: std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        // this loop inserts cache for ids from 0 -> 9
        for i in 0..10 {
            throttler.cache_message(&i, &params).await.unwrap();
        }

        // this loop inserts cache for ids from 0 -> 8
        for i in 0..9 {
            throttler.cache_message(&i, &params).await.unwrap();
        }

        // this line inserts cache for id 10, while capacity is full, a twin should be removed from cache
        // the only twin with the least frequency is twin 9
        throttler.cache_message(&10, &params).await.unwrap();

        // since twin 9 is removed from cache, it should be able to send a message
        assert!(throttler.can_send_message(&9, &params).await.unwrap())
    }

    #[tokio::test]
    async fn test_valid_rate() {
        valid_rate(CleanUp::SlidingWindow).await;
        valid_rate(CleanUp::FixedWindow).await;
    }
    async fn valid_rate(clean_up: CleanUp) {
        let lfu = LFUCache::new(clean_up, 10, 60, 100, 100);
        let throttler = Throttler::new(lfu);
        let params = Params {
            size: 1,
            timestamp: std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        for _ in 0..10 {
            throttler.cache_message(&1, &params).await.unwrap();
        }

        assert!(throttler.can_send_message(&1, &params).await.unwrap())
    }
}
