mod redis_storage;

use crate::types::{self, EnvelopeExt};
use crate::types::{Backlog, Envelope};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bb8_redis::redis;
pub use redis::*;
pub use redis_storage::RedisStorage;
use serde::{Deserialize, Serialize};

// operation against backlog
#[async_trait]
pub trait Storage: Clone + Send + Sync + 'static {
    // track stores some information about the envelope
    // in a backlog used to track replies received related to this
    // envelope. The envelope has to be a request envelope.
    async fn track(&self, uid: &str, ttl: u64, backlog: Backlog) -> Result<()>;

    // gets message with ID. This will retrieve the object
    // from backlog.$id. On success, this can either be None which means
    // there is no message with that ID or the actual message.
    async fn get(&self, uid: &str) -> Result<Option<Backlog>>;

    // pushes the message to local process (msgbus.$cmd) queue.
    // this means the message will be now available to the application
    // to process.
    //
    // KNOWN ISSUE: we should not set TTL on this queue because
    // we are not sure how long the application will take to process
    // all it's queues messages. So this is potentially dangerous. A harmful
    // twin can flood a server memory by sending many large messages to a `cmd`
    // that is not handled by any application.
    //
    // SUGGESTED FIX: instead of setting TTL on the $cmd queue we can limit the length
    // of the queue. So for example, we allow maximum of 500 message to be on this queue
    // after that we need to trim the queue to specific length after push (so drop older messages)
    async fn run(&self, msg: JsonRequest) -> Result<()>;

    // pushed a json response back to the caller according to his
    // reply queue.
    async fn reply(&self, queue: &str, response: JsonResponse) -> Result<()>;

    // messages waits on either "requests" or "replies" that are available to
    // be send to remote twin.
    async fn messages(&self) -> Result<JsonMessage>;
}

pub enum JsonMessage {
    Request(JsonRequest),
    Response(JsonResponse),
}

impl From<JsonRequest> for JsonMessage {
    fn from(value: JsonRequest) -> Self {
        Self::Request(value)
    }
}

impl From<JsonResponse> for JsonMessage {
    fn from(value: JsonResponse) -> Self {
        Self::Response(value)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonError {
    pub code: u32,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonRequest {
    #[serde(rename = "ver")]
    pub version: usize,
    #[serde(rename = "ref")]
    pub reference: Option<String>,
    #[serde(rename = "cmd")]
    pub command: String,
    #[serde(rename = "exp")]
    pub expiration: u64,
    #[serde(rename = "dat")]
    pub data: String,
    #[serde(rename = "tag")]
    pub tags: Option<String>,
    #[serde(rename = "src")]
    pub source: u32,
    #[serde(rename = "dst")]
    pub destinations: Vec<u32>,
    #[serde(rename = "ret")]
    pub reply_to: String,
    #[serde(rename = "shm")]
    pub schema: String,
    #[serde(rename = "now")]
    pub timestamp: u64,
}

impl JsonRequest {
    /// parts return all the components of this message. this include a backlog
    /// object with all the tracking information, and all envelopes (one for)
    /// each destination. each envelope is already stamped with correct time
    /// stamps, but missing source, and signature information
    /// return (backlog, envelopes, ttl) where ttl is time to live
    /// for the request
    pub fn parts(self) -> Result<(Backlog, Vec<Envelope>, u64)> {
        // create a backlog tracker.
        // that's the part of the request that stays locally
        let mut backlog = types::Backlog::new();
        backlog.reply_to = self.reply_to;
        backlog.reference = self.reference.unwrap_or_default();

        // create an (incomplete) envelope
        let mut request = types::Request::new();

        request.command = self.command;
        request.data = base64::decode(self.data).context("invalid data base64 encoding")?;

        let mut env = Envelope::new();

        env.tags = self.tags;
        env.timestamp = self.timestamp;
        env.expiration = self.expiration;
        env.signature = None;
        env.set_request(request);

        env.stamp();
        let ttl = env.ttl().context("request has expired")?.as_secs();

        let mut envs: Vec<Envelope>;
        if self.destinations.len() == 1 {
            env.destination = self.destinations[0];
            envs = vec![env]
        } else {
            envs = Vec::default();
            for dest in self.destinations {
                env.destination = dest;
                envs.push(env.clone());
            }
        }

        Ok((backlog, envs, ttl))
    }
}
impl redis::ToRedisArgs for JsonRequest {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let bytes = serde_json::to_vec(self).expect("failed to json encode message");
        out.write_arg(&bytes);
    }
}

impl redis::FromRedisValue for JsonRequest {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        if let redis::Value::Data(data) = v {
            serde_json::from_slice(data).map_err(|e| {
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

impl TryFrom<Envelope> for JsonRequest {
    type Error = anyhow::Error;
    fn try_from(mut value: Envelope) -> Result<Self, Self::Error> {
        if !value.has_request() {
            anyhow::bail!("envelope doesn't hold a request");
        }
        let req = value.take_request();

        Ok(JsonRequest {
            version: 1,
            reference: Some(value.uid),
            command: req.command,
            expiration: value.expiration,
            data: base64::encode(&req.data),
            tags: value.tags,
            source: value.source,
            destinations: vec![value.destination],
            reply_to: String::default(),
            schema: String::default(),
            timestamp: value.timestamp,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonResponse {
    #[serde(rename = "ver")]
    pub version: usize,
    #[serde(rename = "ref")]
    pub reference: String,
    #[serde(rename = "dat")]
    pub data: String,
    #[serde(rename = "dst")]
    pub destination: u32,
    // #[serde(rename = "shm")]
    // pub schema: String,
    #[serde(rename = "now")]
    pub timestamp: u64,
    #[serde(rename = "err")]
    pub error: Option<JsonError>,
}

impl redis::ToRedisArgs for JsonResponse {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let bytes = serde_json::to_vec(self).expect("failed to json encode message");
        out.write_arg(&bytes);
    }
}

impl redis::FromRedisValue for JsonResponse {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        if let redis::Value::Data(data) = v {
            serde_json::from_slice(data).map_err(|e| {
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

impl TryFrom<Envelope> for JsonResponse {
    type Error = anyhow::Error;
    fn try_from(env: Envelope) -> Result<Self, Self::Error> {
        use types::envelope::Message;
        use types::response::Body;

        let response = match env.message {
            None => anyhow::bail!("invalid envelope has no message"),
            Some(Message::Request(_)) => anyhow::bail!("invalid envelope has request message"),
            Some(Message::Response(response)) => response,
        };

        let body = response.body.context("reply has no body")?;

        let response = JsonResponse {
            version: 1,
            reference: String::default(),
            data: if let Body::Reply(ref reply) = body {
                base64::encode(&reply.data)
            } else {
                "".into()
            },
            destination: env.destination,
            timestamp: env.timestamp,
            error: if let Body::Error(err) = body {
                Some(JsonError {
                    code: err.code,
                    message: err.message,
                })
            } else {
                None
            },
        };

        Ok(response)
    }
}

impl TryFrom<JsonResponse> for Envelope {
    type Error = anyhow::Error;

    fn try_from(value: JsonResponse) -> Result<Self, Self::Error> {
        let mut response = types::Response::new();

        match value.error {
            None => {
                let mut body = types::Reply::new();
                body.data = base64::decode(value.data)?;
                response.set_reply(body);
            }
            Some(err) => {
                let mut body = types::Error::new();
                body.code = err.code;
                body.message = err.message;
                response.set_error(body);
            }
        };

        let mut env = Envelope::new();

        env.uid = value.reference;
        env.timestamp = value.timestamp;
        env.expiration = 3600; // a response has a fixed timeout
        env.destination = value.destination;
        env.set_response(response);

        Ok(env)
    }
}
