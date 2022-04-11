use super::Message;
use anyhow::Result;

pub trait Identity: Clone {
    fn id() -> u64;
    fn sign(msg: Message) -> Result<Message>;
}

#[derive(Clone)]
pub struct EdIdentity {}

impl Identity for EdIdentity {
    fn id() -> u64 {
        unimplemented!()
    }

    fn sign(_msg: Message) -> Result<Message> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct SrIdentity {}

impl Identity for SrIdentity {
    fn id() -> u64 {
        unimplemented!()
    }

    fn sign(_msg: Message) -> Result<Message> {
        unimplemented!()
    }
}
