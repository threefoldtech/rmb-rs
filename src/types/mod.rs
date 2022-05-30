//use std::io::Write;
use crate::identity::{Identity, Signer, SIGNATURE_LENGTH};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bb8_redis::redis;
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

    pub fn from_json(json: &Vec<u8>) -> serde_json::Result<Self> {
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

        self.signature = hex::encode(signature);
    }

    pub fn verify<I: Identity>(&mut self, identity: &I) -> Result<()> {
        let digest = self.challenge().unwrap();
        let signature = hex::decode(&self.signature).context("failed to decode signature")?;

        if signature.len() != SIGNATURE_LENGTH {
            bail!("invalid signature length")
        }

        if !identity.verify(&signature, &digest[..]) {
            bail!("signature verification failed")
        }

        Ok(())
    }
}

impl TryFrom<Vec<u8>> for Message {
    type Error = serde_json::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Message::from_json(&value)
    }
}

impl TryInto<Vec<u8>> for Message {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        self.to_json()
    }
}

impl redis::ToRedisArgs for Message {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let ret = self.to_json();

        match ret {
            Ok(bytes) => out.write_arg(&bytes),
            Err(err) => {
                log::debug!("cannot encode message of {:?} to redis args: {}", self, err);
            }
        }
    }
}

impl redis::FromRedisValue for Message {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        if let redis::Value::Data(data) = v {
            let ret = Message::from_json(data);
            match ret {
                Ok(bytes) => Ok(bytes),
                Err(err) => Err(redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "cannot decode a message from json {}",
                    err.to_string(),
                ))),
            }
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "expected a data type from redis",
            )))
        }
    }
}
