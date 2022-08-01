use super::upload::UploadConfig;

use crate::identity::Identity;
use crate::storage::Storage;
use crate::twin::TwinDB;

/// - This is an example struct which holds the shared parameters between all handlers
/// such as the database or cache
/// - You can build your own AppData object or change this model

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
