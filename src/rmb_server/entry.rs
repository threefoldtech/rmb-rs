use super::routes::*;
use anyhow::Result;
use hyper::{
    service::{make_service_fn, service_fn},
    Server,
};

pub struct RmbServer<'a> {
    addr: &'a str,
    port: u16,
}

impl<'a> RmbServer<'a> {
    pub fn new(addr: &'a str, port: u16) -> Self {
        RmbServer { addr, port }
    }

    #[tokio::main]
    pub async fn run(&self) -> Result<()> {
        let addr = format!("{}:{}", self.addr, self.port).parse().unwrap();

        let services = make_service_fn(move |_| async {
            Ok::<_, anyhow::Error>(service_fn(move |req| routes(req)))
        });

        let server = Server::bind(&addr).serve(services);
        println!("Listening on: {}", addr);

        server.await?;

        Ok(())
    }
}
