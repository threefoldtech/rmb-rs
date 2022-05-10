use anyhow::{Ok, Result};
use crate::types::Message;
use parity_scale_codec::Decode;


#[derive(Decode)]
pub struct Twin
{
    pub id: u64,
    pub pk: String,
    pub address: String, // we use string not IP because the twin address can be a dns name
}

impl Twin
{

    pub async fn verify(&self, _msg: &Message) -> Result<()> {
        Ok(())
    }

}