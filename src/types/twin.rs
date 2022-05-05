use anyhow::{Ok, Result};

use super::Message;

#[allow(dead_code)]
pub struct Twin {
    id: u64,
    address: String, // we use string not IP because the twin address can be a dns name
}

#[allow(dead_code)]
impl Twin {
    pub fn new(id: u64, address: String) -> Self {
        Twin { id, address }
    }
    pub async fn id(&self) -> u64 {
        self.id
    }

    pub async fn verify(&self, _msg: &Message) -> Result<()> {
        Ok(())
    }

    pub async fn address(&self) -> String {
        self.address.clone()
    }
}
