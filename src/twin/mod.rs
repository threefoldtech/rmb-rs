mod registrar;

use anyhow::Result;
use async_trait::async_trait;
pub use registrar::*;
use serde::{Deserialize, Serialize};
use sp_core::crypto::AccountId32;
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    str::FromStr,
};

#[async_trait]
pub trait TwinDB: Send + Sync + Clone + 'static {
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>>;
    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>>;
    async fn set_twin(&self, twin: Twin) -> Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Twin {
    pub id: u32,
    pub account: AccountId32,
    pub relay: Option<RelayDomains>,
    pub pk: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, Eq)]
pub struct RelayDomains(HashSet<String>);

impl FromStr for RelayDomains {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let set = s
            .split('_')
            .map(|s| s.to_string())
            .collect::<HashSet<String>>();
        Ok(RelayDomains(set))
    }
}

impl Display for RelayDomains {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = self
            .0
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join("_");
        write!(f, "{}", s)
    }
}

impl RelayDomains {
    pub fn new(inner: &[String]) -> Self {
        Self(inner.iter().cloned().collect())
    }

    pub fn contains(&self, domain: &str) -> bool {
        self.0.contains(domain)
    }

    pub fn iter(&self) -> std::collections::hash_set::Iter<String> {
        self.0.iter()
    }
}
