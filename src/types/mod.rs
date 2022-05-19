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

// #[async_trait]
// impl Work for QueuedMessage {
//     type Job = Self;
//     async fn run(&self, ) {
//         let req = Request::builder()
//             .method(Method::POST)
//             .header("content-type", "application/json");

//         let (req, msg) = match self {
//             QueuedMessage::Forward(msg) => (req.uri("forward uri"), msg),
//             QueuedMessage::Reply(msg) => (req.uri("reply uri"), msg),
//         };

//         let req = req.body(Body::from(serde_json::to_vec(msg).unwrap()));

//         let req = match req {
//             Ok(req) => req,
//             Err(err) => {
//                 todo!()
//             }
//         };

//         let client = Client::new();
//         let _resp = client.request(req).await;
//     }
// }

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
