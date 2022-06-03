mod substrate;

use anyhow::Result;
use async_trait::async_trait;
use parity_scale_codec::Decode;
use serde::{Deserialize, Serialize};
use sp_core::crypto::AccountId32;

pub use substrate::*;

#[async_trait]
pub trait TwinDB: Send + Sync + 'static {
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>>;
    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<u32>;
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
