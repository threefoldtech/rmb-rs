mod substrate;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    str::FromStr,
};
pub use substrate::*;
use subxt::utils::AccountId32;

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    derive_more::From,
    derive_more::Into,
    Serialize,
    Deserialize,
    derive_more::Display,
    Hash,
)]
#[display("{_0}")]
pub struct TwinID(u32);

impl TwinID {
    pub const EMPTY: TwinID = TwinID(0);
}

#[async_trait]
pub trait TwinDB: Send + Sync + Clone + 'static {
    async fn get_twin(&self, twin_id: TwinID) -> Result<Option<Twin>>;
    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>>;
    async fn set_twin(&self, twin: Twin) -> Result<()>;
}

use tfchain_client::client::Twin as TwinData;

use crate::tfchain;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Twin {
    pub id: TwinID,
    pub account: AccountId32,
    pub relay: Option<RelayDomains>,
    pub pk: Option<Vec<u8>>,
}

impl From<TwinData> for Twin {
    fn from(twin: TwinData) -> Self {
        Twin {
            id: twin.id.into(),
            account: twin.account_id,
            relay: twin.relay.map(|v| {
                let string: String = String::from_utf8_lossy(&v.0).into();
                RelayDomains::from_str(&string).unwrap_or_default()
            }),
            pk: twin.pk.map(|v| v.0),
        }
    }
}

impl From<tfchain::Twin> for Twin {
    fn from(twin: tfchain::Twin) -> Self {
        Twin {
            id: twin.id.into(),
            account: twin.account_id,
            relay: twin.relay.map(|v| {
                let string: String = String::from_utf8_lossy(&v.0).into();
                RelayDomains::from_str(&string).unwrap_or_default()
            }),
            pk: twin.pk.map(|v| v.0),
        }
    }
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
