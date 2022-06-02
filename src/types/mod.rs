//use std::io::Write;
use crate::identity::{Identity, Signer, SIGNATURE_LENGTH};
use anyhow::{Context, Result};
use bb8_redis::redis;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::time::{Duration, SystemTime};

#[derive(Clone)]
pub enum TransitMessage {
    Request(Message),
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
    pub expiration: u64,
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
    pub timestamp: u64,
    #[serde(rename = "err")]
    pub error: Option<String>,
    #[serde(rename = "sig")]
    pub signature: Option<String>,
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
            timestamp: Default::default(),
            error: None,
            signature: Default::default(),
        }
    }
}

impl Message {
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    pub fn from_json(json: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(json)
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
        write!(hash, "{}", self.timestamp)?;
        // this is for backward compatibility
        // this replaces the `proxy` flag which is
        // no obsolete
        write!(hash, "false")?;

        Ok(hash.compute())
    }

    /// sign the message with given signer
    pub fn sign<S: Signer>(&mut self, signer: &S) {
        // we do unwrap because this should never fail.
        let digest = self.challenge().unwrap();
        let signature = signer.sign(&digest[..]);

        self.signature = Some(hex::encode(signature));
    }

    /// verify the message signatre
    pub fn verify<I: Identity>(&mut self, identity: &I) -> Result<()> {
        let signature = match self.signature {
            Some(ref sig) => sig,
            None => bail!("message is not signed"),
        };
        let digest = self.challenge().unwrap();
        let signature = hex::decode(signature).context("failed to decode signature")?;

        if signature.len() != SIGNATURE_LENGTH {
            bail!("invalid signature length")
        }

        if !identity.verify(&signature, &digest[..]) {
            bail!("signature verification failed")
        }

        Ok(())
    }

    /// stamp sets the correct timestamp on the message. This is done
    /// by validating the stamp set by the user so if the stamp is
    /// in the future, the stamp is updated to `now`.
    pub fn stamp(&mut self) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if self.timestamp > now {
            self.timestamp = now;
        }
    }

    /// ttl returns the time to live of this message
    /// based it the timestamp expiration value and ttl
    pub fn ttl(&self) -> Option<Duration> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // to compute the ttl we need to do the following
        // - ttl = expiration - (now - msg.timestamp)
        match now.checked_sub(self.timestamp) {
            Some(d) => self.expiration.checked_sub(d).map(Duration::from_secs),
            None => None,
        }
    }

    /// generic validation on the message
    pub fn valid(&self) -> Result<()> {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        // ts has to be in the future of self.now
        let du = match ts.checked_sub(Duration::from_secs(self.timestamp)) {
            Some(du) => du,
            None => bail!("message 'now' is in the future"),
        };
        if du.as_secs() > 60 {
            bail!("message is too old ('{}' seconds)", du.as_secs());
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
        let bytes = self.to_json().expect("failed to json encode message");
        out.write_arg(&bytes);
    }
}

impl redis::FromRedisValue for Message {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        if let redis::Value::Data(data) = v {
            Message::from_json(data).map_err(|e| {
                redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "cannot decode a message from json {}",
                    e.to_string(),
                ))
            })
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "expected a data type from redis",
            )))
        }
    }
}
