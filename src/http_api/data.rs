use crate::cache::RedisCache;
use crate::{identity::Identity, storage::Storage};
use crate::twin::{SubstrateTwinDB, TwinDB};

/// - This is an example struct which holds the shared parameters between all handlers
/// such as the database or cache
/// - You can build your own AppData object or change this model
///
/// Initialization Example
///
/// ```
/// let r = RedisStorage;
/// let i = SubstrateIdentity;
///
/// let d = AppData {
///     storage: &r,
///     identity: &i,
/// };
///
/// ```

#[allow(dead_code)]
#[derive(Clone)]
pub struct AppData<S, I>
where
    S: Storage,
    I: Identity,
{
    pub storage: S,
    pub identity: I,
    pub twin_id: u32,
}
