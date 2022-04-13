use http_api::{AppData, HttpApi};
use storage::RedisStorage;
use types::Ed25519Identity;

mod http_api;
mod storage;
mod types;

#[tokio::main]
async fn main() {
    let app_data = AppData {
        storage: RedisStorage,
        identity: Ed25519Identity::try_from("value").unwrap(),
    };

    HttpApi::new("127.0.0.1", 888)
        .unwrap()
        .run(app_data)
        .await
        .unwrap();
}
