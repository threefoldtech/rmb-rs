use crate::identity::{Identity, Signer};
use anyhow::Result;
use bb8_redis::redis;
use protobuf::Message;
use std::io::Write;
use std::time::{Duration, SystemTime};

include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

pub use types::*;

pub trait EnvelopeExt: Challengeable {
    /// sign the message with given signer
    fn sign<S: Signer>(&mut self, signer: &S);

    /// verify the message signature
    fn verify<I: Identity>(&self, identity: &I) -> Result<()>;

    fn stamp(&mut self);

    /// ttl returns the time to live of this message
    /// based it the timestamp expiration value and ttl
    fn ttl(&self) -> Option<Duration>;

    /// generic validation on the message
    fn valid(&self) -> Result<()> {
        if self.ttl().is_none() {
            bail!("message has expired");
        }

        Ok(())
    }

    fn age(&self) -> Duration;
}

pub trait Challengeable {
    fn challenge<W: Write>(&self, w: &mut W) -> Result<()>;
}

// a generic sign for any challengeable
pub fn sign<C: Challengeable, S: Signer>(c: &C, signer: &S) -> Vec<u8> {
    let mut hash = md5::Context::new();
    c.challenge(&mut hash).unwrap();
    let hash = hash.compute();
    Vec::from(signer.sign(&hash[..]))
}

// a generic verify for any challengeable
pub fn verify<C: Challengeable, I: Identity>(
    c: &C,
    identity: &I,
    signature: Option<&[u8]>,
) -> Result<()> {
    let signature = match signature {
        Some(sig) => sig,
        None => bail!("message is not signed"),
    };

    let mut hash = md5::Context::new();
    c.challenge(&mut hash)?;
    let digest = hash.compute();

    identity.verify(signature, &digest[..])
}

impl<T> Challengeable for &[T]
where
    T: std::fmt::Display,
{
    fn challenge<W: Write>(&self, w: &mut W) -> Result<()> {
        for v in self.iter() {
            write!(w, "{}", v)?;
        }

        Ok(())
    }
}

impl EnvelopeExt for Envelope {
    /// sign the message with given signer
    fn sign<S: Signer>(&mut self, signer: &S) {
        self.signature = Some(sign(self, signer));
    }

    /// verify the message signature
    fn verify<I: Identity>(&self, identity: &I) -> Result<()> {
        verify(self, identity, self.signature.as_deref())
    }

    /// stamp sets the correct timestamp on the message.
    /// - first validate the timestamp set by a client if in the future, it's reset to now
    /// - if the timestamp is (now) or in the past. the timestamp is updated also to now
    ///   but the expiration period is recalculated so the message deadline does not change
    fn stamp(&mut self) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        (self.timestamp, self.expiration) = stamp(now, self.timestamp, self.expiration);
    }

    /// ttl returns the time to live of this message
    /// based it the timestamp expiration value and ttl
    fn ttl(&self) -> Option<Duration> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        ttl(now, self.timestamp, self.expiration)
    }

    /// age returns the now - message.timestamp
    /// this will give how old the message was when
    /// it was last stamped.
    /// if timestamp is in the future, age will be 0
    fn age(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // to compute the ttl we need to do the following
        // - ttl = expiration - (now - msg.timestamp)
        Duration::from_secs(now.saturating_sub(self.timestamp))
    }
}

impl redis::ToRedisArgs for Envelope {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let bytes = self.write_to_bytes().expect("failed to encode envelope");
        out.write_arg(&bytes);
    }
}

impl redis::FromRedisValue for Envelope {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        if let redis::Value::Data(data) = v {
            Self::parse_from_bytes(data).map_err(|e| {
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

impl redis::ToRedisArgs for Backlog {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let bytes = self.write_to_bytes().expect("failed to encode envelope");
        out.write_arg(&bytes);
    }
}

impl redis::FromRedisValue for Backlog {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        if let redis::Value::Data(data) = v {
            Self::parse_from_bytes(data).map_err(|e| {
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

impl Challengeable for types::Request {
    fn challenge<W: Write>(&self, hash: &mut W) -> Result<()> {
        write!(hash, "{}", self.command)?;
        hash.write_all(&self.data)?;

        Ok(())
    }
}

impl Challengeable for types::Response {
    fn challenge<W: Write>(&self, hash: &mut W) -> Result<()> {
        if self.has_error() {
            let err = self.error();
            write!(hash, "{}", err.code)?;
            write!(hash, "{}", err.message)?;
        } else {
            let reply = self.reply();
            hash.write_all(&reply.data)?;
        }

        Ok(())
    }
}

impl Challengeable for Envelope {
    fn challenge<W: Write>(&self, hash: &mut W) -> Result<()> {
        write!(hash, "{}", self.uid)?;
        if let Some(ref tags) = self.tags {
            write!(hash, "{}", tags)?;
        }

        write!(hash, "{}", self.timestamp)?;
        write!(hash, "{}", self.source)?;
        write!(hash, "{}", self.destination)?;

        if let Some(ref message) = self.message {
            match message {
                types::envelope::Message::Request(req) => req.challenge(hash)?,
                types::envelope::Message::Response(resp) => resp.challenge(hash)?,
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use crate::identity::{Identity, Sr25519Signer};

    use super::{Envelope, EnvelopeExt};

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

    #[test]
    fn sign_verify() {
        let signer = Sr25519Signer::try_from("//Alice").unwrap();

        let account = signer.account();

        let mut env = Envelope::new();

        env.sign(&signer);

        env.verify(&account).unwrap();
    }
}
