use rmb_server::RmbServer;

mod rmb_server;

fn main() {
    RmbServer::new("127.0.0.1", 888).run().unwrap();
}
