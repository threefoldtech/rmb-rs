use super::AppData;
use crate::{storage::Storage, types::Identity};
use anyhow::Result;
use hyper::{
    service::{make_service_fn, service_fn},
    Server,
};
use hyper::{Body, Method, Request, Response, StatusCode};
use std::marker::{Send, Sync};
use std::{net::SocketAddr, sync::Arc};

pub struct HttpApi {
    addr: SocketAddr,
}

impl HttpApi {
    pub fn new<T: AsRef<str>>(ip: T, port: u16) -> Result<Self> {
        let ip = ip.as_ref().parse()?;
        let addr = SocketAddr::new(ip, port);
        Ok(HttpApi { addr })
    }

    pub async fn run<S, I>(&self, app_data: AppData<S, I>) -> Result<()>
    where
        S: 'static + Storage + Send + Sync,
        I: 'static + Identity + Send + Sync,
    {
        let data = Arc::new(app_data);

        let services = make_service_fn(move |_| {
            let data = data.clone();
            let service = service_fn(move |req| routes(req, data.clone()));
            async move { Ok::<_, anyhow::Error>(service) }
        });

        let server = Server::bind(&self.addr).serve(services);
        println!("Listening on: {}", self.addr.ip());

        server.await?;

        Ok(())
    }
}

pub async fn rmb_remote<S: Storage, I: Identity>(
    _req: Request<Body>,
    _data: Arc<AppData<S, I>>,
) -> Result<Response<Body>> {
    Ok(Response::new("RmbRemote Endpoint".into()))
}

pub async fn rmb_reply<S: Storage, I: Identity>(
    _req: Request<Body>,
    _data: Arc<AppData<S, I>>,
) -> Result<Response<Body>> {
    Ok(Response::new("RmbReply Endpoint".into()))
}

pub async fn routes<S: Storage, I: Identity>(
    req: Request<Body>,
    data: Arc<AppData<S, I>>,
) -> Result<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/rmb-remote") => rmb_remote(req, data.clone()).await,
        (&Method::POST, "/rmb-reply") => rmb_reply(req, data.clone()).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()),
    }
}
