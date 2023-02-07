# Throttler

- Throttler's job is to limit each twins's access to the relay.
- A twin could send `X` messages per minute to the relay.
- A twin could send `Y` bytes per minute to the relay.
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
    trait ThrottlerCache{
        // adds request to cache and decides whether this request exceeded its limits or not 
        has_exceeded_limits()
    }

```


## LFUCache

- should store two hashmaps
- one map maps twins to deque of requests params, accumulative size, and frequency, pointer to twin entry in frequency map
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
    pub struct LFUCache{
        twins_map: HashMap<twin_id, (VecDeque<Params>, total_size, frequency)>,
        frequency_map: BTreeMap<frequency, HashSet<twin_id>>,
        capacity: u32,
    }

```
