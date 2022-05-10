mod twin;
mod substrate_twindb;

pub use twin::*;
pub use substrate_twindb::*;
use anyhow::{Ok, Result};
use async_trait::async_trait;
use crate::types::Message;
use parity_scale_codec::Decode;

#[async_trait]
pub trait TwinDB {
    async fn get(&self, twin_id: u32) -> Result<Twin>;
}

#[async_trait]
pub trait Cache<T> {
    async fn cache(&self, c: T);
}




#[derive(Decode)]
pub struct Twin {
    pub id: u64,
    pub pk: String,
    pub address: String, // we use string not IP because the twin address can be a dns name
}

impl Twin {
    pub async fn verify(&self, _msg: &Message) -> Result<()> {
        Ok(())
    }
}
