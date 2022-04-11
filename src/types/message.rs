use serde::{Deserialize, Serialize};

#[allow(dead_code)]
pub enum QueuedMessage {
    Forward(Message),
    Reply(Message),
}

#[derive(Serialize, Deserialize)]
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
    pub err: String,
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
        }
    }
}

impl Message {}
