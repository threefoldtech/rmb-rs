mod substrate_twindb;

use crate::types::Message;
use anyhow::{Ok, Result};
use async_trait::async_trait;
use bb8_redis::redis::FromRedisValue;
use parity_scale_codec::Decode;
use serde::{Deserialize, Serialize};
pub use substrate_twindb::*;

#[async_trait]
pub trait TwinDB {
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>>;
}

#[derive(Clone, Decode, Serialize, Deserialize, PartialEq, Debug)]
pub struct EntityProof {
    pub id: u32,
    pub signature: String,
}

#[derive(Clone, Decode, Serialize, Deserialize, PartialEq, Debug)]
pub struct Twin {
    pub version: u32,
    pub id: u32,
    pub account: sp_core::crypto::AccountId32,
    pub address: String, // we use string not IP because the twin address can be a dns name
    pub entities: Vec<EntityProof>,
}

impl Twin {
    pub async fn verify(&self, _msg: &Message) -> Result<()> {
        Ok(())
    }
}
