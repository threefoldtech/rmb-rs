//use std::io::Write;
use crate::identity::{Identity, Signer};
use anyhow::{Context, Result};
use async_trait::async_trait;
use hyper::{Body, Client, Method, Request, Uri};
use serde::{Deserialize, Serialize};
use std::io::Write;

#[allow(dead_code)]
#[derive(Clone)]
pub enum QueuedMessage {
    Forward(Message),
    Reply(Message),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    #[serde(rename = "ver")]
    pub version: usize,
    #[serde(rename = "uid")]
    pub id: String,
    #[serde(rename = "cmd")]
    pub command: String,
    #[serde(rename = "exp")]
    pub expiration: usize,
    #[serde(rename = "try")]
    pub retry: usize,
    #[serde(rename = "dat")]
    pub data: String,
    #[serde(rename = "src")]
    pub source: u32,
    #[serde(rename = "dst")]
    pub destination: Vec<u32>,
    #[serde(rename = "ret")]
    pub reply: String,
    #[serde(rename = "shm")]
    pub schema: String,
    #[serde(rename = "now")]
    pub now: usize,
    #[serde(rename = "err")]
    pub error: Option<String>,
    #[serde(rename = "sig")]
    pub signature: String,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            version: 1,
            id: Default::default(),
            command: Default::default(),
            expiration: Default::default(),
            retry: Default::default(),
            data: Default::default(),
            source: Default::default(),
            destination: Default::default(),
            reply: Default::default(),
            schema: Default::default(),
            now: Default::default(),
            error: None,
            signature: Default::default(),
        }
    }
}

impl Message {
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    pub fn from_json(json: Vec<u8>) -> serde_json::Result<Self> {
        serde_json::from_slice(&json)
    }

    fn challenge(&self) -> Result<md5::Digest> {
        let mut hash = md5::Context::new();
        write!(hash, "{}", self.version)?;
        write!(hash, "{}", self.id)?;
        write!(hash, "{}", self.command)?;
        write!(hash, "{}", self.data)?;
        write!(hash, "{}", self.source)?;
        for id in &self.destination {
            write!(hash, "{}", *id)?;
        }
        write!(hash, "{}", self.reply)?;
        write!(hash, "{}", self.now)?;

        Ok(hash.compute())
    }

    pub fn sign<S: Signer>(&mut self, signer: &S) {
        // we do unwrap because this should never fail.
        let digest = self.challenge().unwrap();
        let signature = signer.sign(&digest[..]);

        // todo!: we need to include the signer type in the signature
        // because remote twin will not have no idea how to interpret
        // the twin account id
        self.signature = hex::encode(signature);
    }

    pub fn verify<I: Identity>(&mut self, identity: &I) -> Result<()> {
        let digest = self.challenge().unwrap();
        let signature = hex::decode(&self.signature).context("failed to decode signature")?;

        if !identity.verify(&signature, &digest[..]) {
            bail!("invalid signature")
        }

        Ok(())
    }
}
