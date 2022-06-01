use crate::identity::Ed25519Signer;
use crate::{
    identity::Identity,
    twin::{Twin, TwinDB},
};
use std::collections::HashMap;

#[derive(Default, Clone)]
struct InMemoryDB {
    pub twins: HashMap<u32, Twin>,
}

impl InMemoryDB {
    fn add(&mut self, twin: Twin) {
        self.twins.insert(twin.id, twin);
    }
}

#[async_trait::async_trait]
impl TwinDB for InMemoryDB {
    async fn get_twin(&self, twin_id: u32) -> anyhow::Result<Option<Twin>> {
        Ok(self.twins.get(&twin_id).map(|t| t.clone()))
    }

    async fn get_twin_with_account(
        &self,
        _account_id: sp_runtime::AccountId32,
    ) -> anyhow::Result<u32> {
        unimplemented!()
    }
}

#[test]
fn test() {
    let mut db = InMemoryDB::default();

    let t1 = Ed25519Signer::try_from("value").unwrap();
    db.add(Twin {
        version: 1,
        id: 1,
        account: t1.account(),
        address: "127.0.0.1:5801".into(),
        entities: vec![],
    });

    let t2 = Ed25519Signer::try_from("value").unwrap();

    db.add(Twin {
        version: 1,
        id: 2,
        account: t2.account(),
        address: "127.0.0.1:5802".into(),
        entities: vec![],
    });

    //
}
