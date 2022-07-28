//use std::io::Write;
use crate::identity::{Identity, Signer, SIGNATURE_LENGTH};
use anyhow::{Context, Result};
use bb8_redis::redis;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug)]
pub enum TransitMessage {
    Request(Message),
    Reply(Message),
    Upload(Message),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UploadRequest {
    pub path: PathBuf,
    pub cmd: String,
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
    #[serde(rename = "tag")]
    pub tag: Option<String>,
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

pub trait Challengeable {
    fn challenge(&self) -> Result<md5::Digest>;
}

// a generic sign for any challengeable
pub fn sign<C: Challengeable, S: Signer>(c: &C, signer: &S) -> String {
    let digest = c.challenge().unwrap();
    let signature = signer.sign(&digest[..]);

    hex::encode(signature)
}

// a generic verify for any challengeable
pub fn verify<C: Challengeable, I: Identity>(
    c: &C,
    identity: &I,
    signature: &Option<String>,
) -> Result<()> {
    let signature = match signature {
        Some(ref sig) => sig,
        None => bail!("message is not signed"),
    };

    let digest = c.challenge()?;
    let signature = hex::decode(signature).context("failed to decode signature")?;

    if signature.len() != SIGNATURE_LENGTH {
        bail!("invalid signature length")
    }

    if !identity.verify(&signature, &digest[..]) {
        bail!("signature verification failed")
    }

    Ok(())
}

impl<T> Challengeable for &[T]
where
    T: std::fmt::Display,
{
    fn challenge(&self) -> Result<md5::Digest> {
        let mut hash = md5::Context::new();
        for v in self.iter() {
            write!(hash, "{}", v)?;
        }

        Ok(hash.compute())
    }
}

impl UploadRequest {
    pub fn new(path: PathBuf, cmd: String) -> Self {
        Self { path, cmd }
    }

    pub fn sign<S: Signer>(&mut self, signer: &S, timestamp: u64, source: u32) -> String {
        let fields = vec![timestamp.to_string(), source.to_string()];
        sign(&fields.as_slice(), signer)
    }

    pub fn verify<I: Identity>(
        &self,
        identity: &I,
        timestamp: u64,
        source: u32,
        signature: String,
    ) -> Result<()> {
        let fields = vec![timestamp.to_string(), source.to_string()];
        verify(&fields.as_slice(), identity, &Some(signature))
    }
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
            tag: None,
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

impl Challengeable for Message {
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
}

impl Message {
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    pub fn from_json(json: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(json)
    }

    /// sign the message with given signer
    pub fn sign<S: Signer>(&mut self, signer: &S) {
        self.signature = Some(sign(self, signer));
    }

    /// verify the message signature
    pub fn verify<I: Identity>(&self, identity: &I) -> Result<()> {
        verify(self, identity, &self.signature)
    }

    /// stamp sets the correct timestamp on the message.
    /// - first validate the timestamp set by a client if in the future, it's reset to now
    /// - if the timestamp is (now) or in the past. the timestamp is updated also to now
    ///   but the expiration period is recalculated so the message deadline does not change
    pub fn stamp(&mut self) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        (self.timestamp, self.expiration) = stamp(now, self.timestamp, self.expiration);
    }

    /// ttl returns the time to live of this message
    /// based it the timestamp expiration value and ttl
    pub fn ttl(&self) -> Option<Duration> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        ttl(now, self.timestamp, self.expiration)
    }

    /// generic validation on the message
    pub fn valid(&self) -> Result<()> {
        if self.ttl().is_none() {
            bail!("message has expired");
        }

        Ok(())
    }

    /// age returns the now - message.timestamp
    /// this will give how old the message was when
    /// it was last stamped.
    /// if timestamp is in the future, age will be 0
    pub fn age(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // to compute the ttl we need to do the following
        // - ttl = expiration - (now - msg.timestamp)
        Duration::from_secs(now.saturating_sub(self.timestamp))
    }
}

impl TryFrom<&Message> for UploadRequest {
    type Error = anyhow::Error;

    fn try_from(msg: &Message) -> Result<Self, Self::Error> {
        let data = base64::decode(&msg.data).with_context(|| "cannot decode message data")?;
        let request: Self =
            serde_json::from_slice(&data).with_context(|| "cannot decode upload request")?;

        if msg.destination.len() > 1 {
            bail!("cannot send upload to multiple destinations");
        }

        if request.cmd.trim().is_empty() {
            bail!("cmd is empty");
        }

        if request.path.is_file() && request.path.exists() {
            Ok(request)
        } else {
            bail!("path does not exist or is not a file")
        }
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

/// based on the now, timestamp and expiration return new
/// timestamp and expiration values so that
/// - timestamp is always now
/// - expiration is set to a value so that the message deadline doesn't change
fn stamp(now: u64, ts: u64, exp: u64) -> (u64, u64) {
    // if timestamp is not set at all, or is in the future
    // we always return the now as the timestamp. expiration
    // then is not touched.
    if ts > now || ts == 0 {
        return (now, exp);
    }

    // we checked above so we sure that du is 0 or higher (no overflow)
    let du = now - ts;

    let exp = exp.saturating_sub(du);

    (now, exp)
}

/// ttl returns duration of ttl based on the given 'now', 'timestamp' and 'expiration'
/// None is returned if ts + exp is before now. otherwise returns how many seconds
/// before expiration
fn ttl(now: u64, ts: u64, exp: u64) -> Option<Duration> {
    match (ts + exp).checked_sub(now) {
        None => None,
        Some(0) => None,
        Some(d) => Some(Duration::from_secs(d)),
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn stamp() {
        use super::stamp;

        let (ts, ex) = stamp(1, 0, 20);
        assert_eq!(ts, 1);
        assert_eq!(ex, 20);

        let (ts, ex) = stamp(1, 10, 20);
        assert_eq!(ts, 1);
        assert_eq!(ex, 20);

        let (ts, ex) = stamp(10, 1, 20);
        assert_eq!(ts, 10);
        assert_eq!(ex, 11);

        let (ts, ex) = stamp(21, 1, 20);
        assert_eq!(ts, 21);
        assert_eq!(ex, 0);

        let (ts, ex) = stamp(30, 1, 20);
        assert_eq!(ts, 30);
        assert_eq!(ex, 0);
    }

    #[test]
    fn ttl() {
        use super::ttl;
        use std::time::Duration;

        let t = ttl(0, 10, 20);
        assert_eq!(t, Some(Duration::from_secs(30)));

        let t = ttl(10, 0, 20);
        assert_eq!(t, Some(Duration::from_secs(10)));

        let t = ttl(20, 0, 20);
        assert_eq!(t, None);

        let t = ttl(30, 0, 20);
        assert_eq!(t, None);
    }
}
