mod substrate_twindb;

use crate::types::Message;
use anyhow::{Ok, Result};
use async_trait::async_trait;
use parity_scale_codec::Decode;
use serde::{Serialize, Deserialize};
pub use substrate_twindb::*;

#[async_trait]
pub trait TwinDB {
    async fn get(&self, twin_id: u32) -> Result<Twin>;
}

#[derive(Clone, Decode, Serialize, Deserialize)]
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
