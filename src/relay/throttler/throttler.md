# Throttler

- Throttler's job is to limit each twins's access to the relay.
- A twin could send `X` messages per time_window to the relay.
- A twin could send `Y` bytes per time_window to the relay.
- If a twin exceeded its limits, the message should be dropped, an error returned to the user, a log about the bad acting twin should be dumped.
- Throttler should use some type of cache to decide whether the twin has exceeded its limits.

```Rust
    struct Throttler{
        cache: ThrottlerCache,
    }
```

## ThrottlerCache

- should be as generic as possible to accept different implementations
- should take into account number of requests per minute and number of bytes per minute per twin.

```Rust
    #[async_trait]
    pub trait ThrottlerCache: Send + Sync + 'static {
        /// can_send_message checks if this twin can send a new massage with the provided paramters
        async fn can_send_message(& self, twin_id: &u32, params: &Params) -> Result<bool>;

        /// cache_message caches the provided paramters of the message
        async fn cache_message(& self, twin_id: &u32, params: &Params) -> Result<()>;
    }

```

## LFUCache

- should store two hashmaps
- one map maps twins to deque of requests params, accumulative size, and frequency.
- one map maps frequency to some container of twins
- performing any operation on any twin happens in the following sequence:
  - acquire twin frequency from twin map
  - remove twin from frequency map, while ensuring that the frequency key points to some non empty container, otherwise delete it.
  - increment frequency
  - do operation
  - add twin to its corresponsing frequency in frequency map
- if cache hit its capacity, a twin should be removed from cache with the following sequence:
  - get least frequency from frequency map
  - randomly select any of the twins
  - remove it from frequency map
  - remove it from twin map

```Rust
    pub struct LFUCache {
        clean_up: CleanUp,
        twins_map: Arc<RwLock<HashMap<u32, TwinCache>>>,
        frequency_map: Arc<RwLock<BTreeMap<u32, HashSet<u32>>>>,
        capacity: usize,
        time_window: u64,
        requests_per_window: usize,
        size_per_window: usize,
    }
```
