use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::workers::Work;

#[allow(dead_code)]
#[derive(Clone)]
pub enum QueuedMessage {
    Forward(Message),
    Reply(Message),
}

#[async_trait]
impl Work for QueuedMessage {
    async fn run(&self) {
        match self {
            QueuedMessage::Forward(msg) => {}
            QueuedMessage::Reply(msg) => {}
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    pub ver: usize,
    pub uid: String,
    pub cmd: String,
    pub exp: usize,

    // original message filed is try
    #[serde(rename = "try")]
    pub retry: usize,

    pub dat: String,
    pub src: usize,
    pub dst: Vec<usize>,
    pub ret: String,
    pub shm: String,
    pub now: usize,
    pub err: String,
    pub sig: String,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            ver: 1,
            uid: Default::default(),
            cmd: Default::default(),
            exp: Default::default(),
            retry: Default::default(),
            dat: Default::default(),
            src: Default::default(),
            dst: Default::default(),
            ret: Default::default(),
            shm: Default::default(),
            now: Default::default(),
            err: Default::default(),
            sig: Default::default(),
        }
    }
}

impl Message {}
