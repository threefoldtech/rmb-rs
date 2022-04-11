use rmb_server::RmbServer;

mod rmb_server;
mod storage;
mod types;

#[tokio::main]
async fn main() {
    RmbServer::new("127.0.0.1", 888).run().await.unwrap();
}
