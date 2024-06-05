use bb8_redis::redis::{ErrorKind, FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value};
use std::fmt::Display;
use std::num::ParseIntError;
use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::types::Address;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid prefix")]
    InvalidPrefix,
    #[error("invalid value: {0}")]
    InvalidValue(#[from] ParseIntError),
}
/// StreamID is a type alias for a user id. can be replaced later
/// but for now we using numeric ids
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct StreamID(u32, Option<String>);

impl StreamID {
    pub fn zero(&self) -> bool {
        self.0 == 0
    }
}

impl Display for StreamID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.1 {
            Some(ref con) => write!(f, "{}:{}", self.0, con),
            None => write!(f, "{}", self.0),
        }
    }
}

impl AsRef<StreamID> for &StreamID {
    fn as_ref(&self) -> &StreamID {
        self
    }
}

impl FromStr for StreamID {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once(':') {
            None => {
                let id: u32 = s.parse()?;
                Ok(Self(id, None))
            }
            Some((id, sid)) => {
                let id: u32 = id.parse()?;

                Ok(Self(id, Some(sid.into())))
            }
        }
    }
}

impl From<&Address> for StreamID {
    fn from(value: &Address) -> Self {
        Self(value.twin, value.connection.clone())
    }
}

impl From<&StreamID> for Address {
    fn from(value: &StreamID) -> Self {
        let mut address = Address::new();
        address.twin = value.0;
        address.connection.clone_from(&value.1);

        address
    }
}

impl PartialEq<protobuf::MessageField<Address>> for StreamID {
    fn eq(&self, other: &protobuf::MessageField<Address>) -> bool {
        match other.0 {
            None => false,
            Some(ref addr) => addr.twin == self.0 && addr.connection == self.1,
        }
    }
}

impl From<&protobuf::MessageField<Address>> for StreamID {
    fn from(value: &protobuf::MessageField<Address>) -> Self {
        Self(value.twin, value.connection.clone())
    }
}

impl From<u32> for StreamID {
    fn from(value: u32) -> Self {
        Self(value, None)
    }
}

impl From<(u32, Option<String>)> for StreamID {
    fn from((id, sid): (u32, Option<String>)) -> Self {
        Self(id, sid)
    }
}

impl ToRedisArgs for StreamID {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + bb8_redis::redis::RedisWrite,
    {
        //out.write_arg("stream:".as_bytes());
        out.write_arg(format!("stream:{}", self).as_bytes());
    }
}

impl FromRedisValue for StreamID {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        if let Value::Data(bytes) = v {
            let s = core::str::from_utf8(bytes)?;

            if !s.starts_with("stream:") {
                return Err(RedisError::from((
                    ErrorKind::TypeError,
                    "stream id must be prefixed with `stream:`",
                )));
            }

            let part = &s[7..];

            let id: StreamID = part.parse().map_err(|err: ParseError| {
                RedisError::from((
                    ErrorKind::TypeError,
                    "stream id parse error",
                    err.to_string(),
                ))
            })?;

            return Ok(id);
        }

        Err((ErrorKind::TypeError, "invalid stream id type").into())
    }
}
/// ConnectionID is a unique id per connection. this way we can tell
/// if a user connection was reset (user lost connection and then reconnected)
/// it's needed to make sure multiple workers don't endup serving the same user
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ConnectionID(u128);

impl ConnectionID {
    pub fn new() -> Self {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap() // todo: may be return an error
            .as_nanos();
        Self(d)
    }
}
/// MessageID is id of last message delivered to a user
#[derive(Default, Debug, Copy, Clone)]
pub struct MessageID(u64, u64);

impl Display for MessageID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
    }
}

impl ToRedisArgs for MessageID {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + bb8_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(format!("{}-{}", self.0, self.1));
    }
}

impl FromStr for MessageID {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('-') {
            None => Ok(Self(s.parse()?, 0)),
            Some((l, r)) => Ok(Self(l.parse()?, r.parse()?)),
        }
    }
}

impl FromRedisValue for MessageID {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        if let Value::Data(bytes) = v {
            let id: MessageID =
                core::str::from_utf8(bytes)?
                    .parse()
                    .map_err(|err: ParseIntError| {
                        RedisError::from((
                            ErrorKind::TypeError,
                            "message id parse error",
                            err.to_string(),
                        ))
                    })?;

            return Ok(id);
        }

        Err((ErrorKind::TypeError, "invalid message-id type").into())
    }
}
/// A connection is defined mainly by a connection id, but it also track
/// las message id received
pub struct Connection(ConnectionID, MessageID);

impl Connection {
    pub fn id(&self) -> &ConnectionID {
        &self.0
    }

    pub fn set_last(&mut self, id: MessageID) {
        self.1 = id;
    }

    pub fn last(&self) -> &MessageID {
        &self.1
    }
}

impl From<ConnectionID> for Connection {
    fn from(value: ConnectionID) -> Self {
        Self(value, MessageID::default())
    }
}

#[cfg(test)]
mod test {
    use super::{MessageID, StreamID};
    use std::num::ParseIntError;

    use bb8_redis::redis::{FromRedisValue, ToRedisArgs, Value};

    #[test]
    fn parse_message_id() {
        let id: MessageID = "0".parse().unwrap();
        assert_eq!(id.0, 0);
        assert_eq!(id.1, 0);

        let id: MessageID = "100-0".parse().unwrap();
        assert_eq!(id.0, 100);
        assert_eq!(id.1, 0);

        let id: MessageID = "100-100".parse().unwrap();
        assert_eq!(id.0, 100);
        assert_eq!(id.1, 100);

        let id: Result<MessageID, ParseIntError> = "x".parse();
        assert!(id.is_err());
    }

    #[test]
    fn parse_stream_id() {
        let id: StreamID = "10".parse().unwrap();
        assert_eq!(id.0, 10);
        assert_eq!(id.1, None);

        let id: StreamID = "10:con".parse().unwrap();
        assert_eq!(id.0, 10);
        assert!(matches!(id.1, Some(ref sid) if sid == "con"));

        let mut arg = id.to_redis_args();
        assert_eq!(arg.len(), 1);

        assert_eq!(String::from_utf8_lossy(&arg[0]), "stream:10:con");

        let v = Value::Data(arg.pop().unwrap());
        let id: StreamID = StreamID::from_redis_value(&v).unwrap();

        assert_eq!(id.0, 10);
        assert!(matches!(id.1, Some(ref sid) if sid == "con"));
    }
}
