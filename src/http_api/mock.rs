use async_trait::async_trait;

use crate::{
    identity::{self, Ed25519Signer, Identity},
    storage::Storage,
    twin::{Twin, TwinDB},
    types::{Message, TransitMessage},
};
use anyhow::Result;
use sp_core::crypto::{AccountId32, Ss58Codec};

#[derive(Clone)]
pub struct StorageMock;

#[async_trait]
impl Storage for StorageMock {
    async fn get(&self, _id: &str) -> Result<Option<Message>> {
        Ok(Some(Message::default()))
    }
    async fn run(&self, _msg: Message) -> Result<()> {
        Ok(())
    }
    async fn forward(&self, _msg: &Message) -> Result<()> {
        Ok(())
    }

    async fn reply(&self, _msg: &Message) -> Result<()> {
        Ok(())
    }
    async fn local(&self) -> Result<Message> {
        Ok(Message::default())
    }
    async fn queued(&self) -> Result<TransitMessage> {
        Ok(TransitMessage::Request(Message::default()))
    }
}
#[derive(Clone)]
pub struct TwinDBMock;

#[async_trait]
impl TwinDB for TwinDBMock {
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>> {
        let mut twin = Twin {
            version: 1,
            id: twin_id,
            account: AccountId32::from_string("5GYtsF9XyaWUEa1zZMhZRe1p9XRMkF21wGyg4G7pPrJok942")
                .unwrap(),
            address: "201:b83b:a0d:24d9:3869:1958:12aa:b24d".to_string(),
            entities: vec![],
        };
        if twin_id == 1 {
            twin.account =
                AccountId32::from_string(&Identities::get_sender_identity().account().to_string())
                    .unwrap();
        } else if twin_id == 2 {
            twin.account =
                AccountId32::from_string(&Identities::get_recv_identity().account().to_string())
                    .unwrap();
        } else if twin_id == 3 {
            twin.account =
                AccountId32::from_string(&Identities::get_fake_identity().account().to_string())
                    .unwrap();
        } else {
            return Ok(None);
        }
        Ok(Some(twin))
    }
    async fn get_twin_with_account(&self, _account_id: AccountId32) -> Result<Option<u32>> {
        unreachable!();
    }
}

pub struct Identities {}
impl Identities {
    pub fn get_sender_identity() -> Ed25519Signer {
        identity::Ed25519Signer::try_from(
            "junior sock chunk accident pilot under ask green endless remove coast wood",
        )
        .unwrap()
    }
    pub fn get_recv_identity() -> Ed25519Signer {
        identity::Ed25519Signer::try_from(
            "spoil critic blossom laundry finger wool enforce mixed also joke narrow pilot",
        )
        .unwrap()
    }
    pub fn get_fake_identity() -> Ed25519Signer {
        identity::Ed25519Signer::try_from(
            "dutch agree conduct uphold absent endorse ticket cloth robot invite know vote",
        )
        .unwrap()
    }
}
