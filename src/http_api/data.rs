use crate::twin::TwinDB;
use crate::{identity::Identity, storage::Storage};

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

#[derive(Clone, Debug)]
pub struct UploadConfig {
    pub enabled: bool,
    pub files_path: String,
}

#[derive(Clone)]
pub struct AppData<S, I, D>
where
    S: Storage,
    I: Identity,
    D: TwinDB,
{
    pub twin: u32,
    pub storage: S,
    pub identity: I,
    pub twin_db: D,
    pub upload_config: UploadConfig,
}
