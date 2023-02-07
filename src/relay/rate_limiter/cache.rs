use super::{Params, ThrottlerCache};
use std::{collections::{BTreeMap, HashMap, HashSet, VecDeque}, sync::Arc};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;

pub struct LFUCache {
    twins_map: Arc<Mutex<HashMap<u32, TwinCache>>>,
    frequency_map: Arc<Mutex<BTreeMap<u32, HashSet<u32>>>>,
    capacity: usize,
    time_window: u64,
    requests_per_window: usize,
    size_per_window: usize,
}

struct TwinCache{
    params: VecDeque<Params>,
    total_size: usize,
    frequency: u32,
}

impl Clone for LFUCache{
    fn clone(&self) -> Self {
        Self { twins_map: self.twins_map.clone(),
            frequency_map: self.frequency_map.clone(), 
            capacity: self.capacity, 
            time_window: self.time_window, 
            requests_per_window: self.requests_per_window, 
            size_per_window: self.size_per_window 
        }
    }
}

impl LFUCache {
    pub fn new(capacity: usize, time_window: u64, requests_per_window: usize, size_per_window: usize) -> Self {
        return Self{
            twins_map: Arc::new(Mutex::new(HashMap::new())),
            frequency_map: Arc::new(Mutex::new(BTreeMap::new())),
            capacity,
            requests_per_window,
            size_per_window,
            time_window,
        }
    }

    /// remove_least_frequently_used removes one of the least frequently used twins from cache
    async fn remove_least_frequently_used(& self) -> Result<()>{
        // first check if capacity is full, return ok if not
        
        let mut frequency_map = self.frequency_map.lock().await;
        let mut entry = match frequency_map.first_entry(){
            Some(entry) => entry,
            None => return Err(anyhow!("frequency map is empty")),
        };
        let twins_set = entry.get_mut();
        let mut to_be_deleted : u32 = 0;
        for twin in twins_set.iter(){
            to_be_deleted = *twin;
            break;
        }
        twins_set.remove(&to_be_deleted);
        
        let mut twins_map = self.twins_map.lock().await;
        twins_map.remove(&to_be_deleted);
        return Ok(())
    }

    /// remove_outdate_data removes twin data that is out of our tracked window
    async fn remove_outdated_data(&self, twin_id: &u32, params: &Params) -> Result<()>{
        let mut twins_map = self.twins_map.lock().await;
        let twin_cache = match twins_map.get_mut(&twin_id){
            Some(cache) => cache,
            None => return Ok(())
        };

        loop{
            let front = twin_cache.params.front();
            match front{
                Some(x) => {
                    if params.timestamp - x.timestamp >= self.time_window{
                        let deleted =twin_cache.params.pop_front().unwrap();
                        twin_cache.total_size -= deleted.size;
                        continue
                    }
                },
                None => break,
            }
        }
        Ok(())
    }

    /// insert_new_twin inserts a new twin into cache
    async fn insert_new_twin(&self, twin_id: &u32, params: &Params) -> Result<()>{
        // check cache capacity
        // if capacity is full, remove least frequently used twin
        // insert new twin with frequency of 1
        let mut twins_map = self.twins_map.lock().await;
        if self.capacity == twins_map.len(){
            self.remove_least_frequently_used().await?;
        }

        if let Some(_) = twins_map.get(twin_id){
            return Err(anyhow!("twin {} is already stored in map", twin_id))
        }
        
        twins_map.insert(*twin_id, TwinCache { 
            params: VecDeque::from([Params{
                size: params.size, 
                timestamp: params.timestamp
            }]), 
            total_size: params.size, 
            frequency: 1 
        });

        let mut frequency_map = self.frequency_map.lock().await;
        if let None = frequency_map.get(&1){
            frequency_map.insert(1, HashSet::new());
        }
        
        let map = frequency_map.get_mut(&1).unwrap();
        map.insert(*twin_id);

        Ok(())
    }

}


#[async_trait]
impl ThrottlerCache for LFUCache{
    async fn can_send_message(&self, twin_id: &u32, params: &Params) -> Result<bool> {
        // checks if message size is larger than limit
        // removes data outside time window
        // checks if number of messages sent in window is below threshold
        if params.size > self.size_per_window{
            return Ok(false)
        }

        self.remove_outdated_data(&twin_id, params).await?;
        
        let twins_map = self.twins_map.lock().await;
        let twins_cache = match twins_map.get(&twin_id){
            Some(cache) => cache,
            None => return Ok(true),
        };
        
        if twins_cache.params.len() == self.requests_per_window || 
            twins_cache.total_size + params.size > self.size_per_window {
            return Ok(false);
        }
        
        return Ok(true)
    }
    async fn cache_message(&self, twin_id: &u32, params: &Params) -> Result<()> {
        // cache message:
        //  1- get twin old frequency
        //  2- remove twin from list in frequency map
        //  3- increase twin frequency
        //  4- add new params
        //  5- add twin to list in frequency map using new frequency
        let mut twins_map = self.twins_map.lock().await;
        let twin_cache = match twins_map.get_mut(&twin_id){
            Some(cache) => cache,
            None => {
                self.insert_new_twin(&twin_id, params).await?;
                return Ok(())
            }
        };

        let old_frequency = twin_cache.frequency;
        // remove old twin from frequency map using old frequency
        let mut frequency_map = self.frequency_map.lock().await;
        let twins_list = match frequency_map.get_mut(&old_frequency){
            Some(list) => list,
            None => return Err(anyhow!("twin {} was not found in frequency map", twin_id)),
        };
        
        if !twins_list.remove(&old_frequency){
            return Err(anyhow!("twin {} was not found in frequency map", twin_id))
        }

        let new_frequency = old_frequency + 1;

        twin_cache.params.push_back(Params { timestamp: params.timestamp, size: params.size });
        twin_cache.total_size += params.size;
        twin_cache.frequency = new_frequency;

        Ok(())
    }
}
