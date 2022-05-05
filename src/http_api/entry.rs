use super::data::AppData;
use crate::{identity::Identity, storage::Storage};
use anyhow::{Context, Result};
use hyper::{
    service::{make_service_fn, service_fn},
    Server,
};
use hyper::{Body, Method, Request, Response, StatusCode};
use std::net::SocketAddr;

pub struct HttpApi<S, I>
where
    S: Storage,
    I: Identity,
{
    addr: SocketAddr,
    data: AppData<S, I>,
}

impl<S, I> HttpApi<S, I>
where
    S: Storage + 'static,
    I: Identity + 'static,
{
    pub fn new<T: AsRef<str>>(ip: T, port: u16, storage: S, identity: I) -> Result<Self> {
        let ip = ip.as_ref().parse().context("failed to parse ip address")?;
        let addr = SocketAddr::new(ip, port);
        Ok(HttpApi {
            addr,
            data: AppData { storage, identity },
        })
    }

    pub async fn run(self) -> Result<()> {
        let services = make_service_fn(move |_| {
            let data = self.data.clone();
            let service = service_fn(move |req| routes(req, data.clone()));
            async move { Ok::<_, anyhow::Error>(service) }
        });

        let server = Server::bind(&self.addr).serve(services);
        info!("listening on: {}", self.addr.ip());

        server.await?;

        Ok(())
    }
}

pub async fn rmb_remote<S: Storage, I: Identity>(
    _req: Request<Body>,
    _data: AppData<S, I>,
) -> Result<Response<Body>> {
    Ok(Response::new("RmbRemote Endpoint".into()))
}

pub async fn rmb_reply<S: Storage, I: Identity>(
    _req: Request<Body>,
    _data: AppData<S, I>,
) -> Result<Response<Body>> {
    Ok(Response::new("RmbReply Endpoint".into()))
}

pub async fn routes<S: Storage, I: Identity>(
    req: Request<Body>,
    data: AppData<S, I>,
) -> Result<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/rmb-remote") => rmb_remote(req, data).await,
        (&Method::POST, "/rmb-reply") => rmb_reply(req, data).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()),
    }
}
