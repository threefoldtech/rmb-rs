use crate::workers::Work;
use async_trait::async_trait;
use hyper::{Body, Client, Method, Request, Uri};
use serde::{Deserialize, Serialize};

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
    #[serde(rename = "retry")]
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

impl Message {}
