use std::sync::Arc;

use crate::{rmb_server::AppData, storage::RedisStorage, types::SrIdentity};

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

    pub async fn run(&self) -> Result<()> {
        let data = AppData {
            storage: RedisStorage {},
            identity: SrIdentity {},
        };

        let data = Arc::new(data);

        // let services = make_service_fn(move |_| {
        //     Ok::<_, anyhow::Error>(service_fn( move |req|routes(req, data.clone())))
        // });

        let services = make_service_fn(move |_| {
            let data = data.clone();
            let service = service_fn(move |req| routes(req, data.clone()));
            async move { Ok::<_, anyhow::Error>(service) }
        });

        let addr = format!("{}:{}", self.addr, self.port).parse()?;

        let server = Server::bind(&addr).serve(services);
        println!("Listening on: {}", addr);

        server.await?;

        Ok(())
    }
}
