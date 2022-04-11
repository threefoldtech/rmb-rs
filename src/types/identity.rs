use super::Message;
use anyhow::Result;

pub trait Identity {
    fn id() -> u64;
    fn sign(msg: Message) -> Result<Message>;
}
