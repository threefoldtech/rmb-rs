use anyhow::{Ok, Result};

use super::Message;

#[allow(dead_code)]
pub struct Twin;

#[allow(dead_code)]
impl Twin {
    pub async fn id() -> u64 {
        unimplemented!()
    }

    pub async fn verify(_msg: &Message) -> Result<()> {
        Ok(())
    }

    pub async fn address() -> String {
        unimplemented!()
    }
}
