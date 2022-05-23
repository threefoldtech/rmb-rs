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
    pub ver: usize,
    pub uid: String,
    pub cmd: String,
    pub exp: usize,
    pub retry: usize,
    pub dat: String,
    pub src: usize,
    pub dst: Vec<usize>,
    pub ret: String,
    pub shm: String,
    pub now: usize,
    pub err: Option<String>,
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
            err: None,
            sig: Default::default(),
        }
    }
}

impl Message {}
