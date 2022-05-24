use super::data::AppData;
use crate::twin::{SubstrateTwinDB, TwinDB};
use crate::{cache::RedisCache, identity::Identity, storage::Storage, types::Message};
use anyhow::{Context, Result};
use hyper::{
    service::{make_service_fn, service_fn},
    Server,
};
use hyper::{Body, Method, Request, Response, StatusCode};
use std::net::SocketAddr;

pub struct HttpApi<S, I, D>
where
    S: Storage,
    I: Identity,
    D: TwinDB + Clone,
{
    addr: SocketAddr,
    data: AppData<S, I, D>,
}

impl<S, I, D> HttpApi<S, I, D>
where
    S: Storage + 'static,
    I: Identity + 'static,
    D: TwinDB + Clone + Send + Sync + 'static,
{
    pub fn new<P: AsRef<str>>(
        ip: P,
        port: u16,
        storage: S,
        identity: I,
        twin_id: u32,
        twin_db: D,
    ) -> Result<Self> {
        let ip = ip.as_ref().parse().context("failed to parse ip address")?;
        let addr = SocketAddr::new(ip, port);
        Ok(HttpApi {
            addr,
            data: AppData {
                storage,
                identity,
                twin_id,
                twin_db,
            },
        })
    }

    pub async fn run(self) -> Result<()> {
        let services = make_service_fn(move |_| {
            let data = self.data.clone();
            let service = service_fn(move |req| routes(req, data.clone()));
            async move { Ok::<_, anyhow::Error>(service) }
        });

        let server = Server::bind(&self.addr).serve(services);
        println!("Server started");
        log::info!("listening on: {}", self.addr.ip());

        server.await?;

        Ok(())
    }
}

pub async fn rmb_remote<S: Storage, I: Identity, D: TwinDB>(
    req: Request<Body>,
    data: AppData<S, I, D>,
) -> Result<Response<Body>> {
    let mut response = Response::new(Body::empty());
    let body = hyper::body::to_bytes(req.into_body()).await;
    let body = match body {
        Ok(body) => body,
        Err(err) => {
            log::debug!("can not extract body because of {}", err);
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };
    let message: Result<Message> = serde_json::from_slice(&body.to_vec())
        .map_err(|err| anyhow::anyhow!(err))
        .context("can not parse body");

    let message = match message {
        Ok(message) => message,
        Err(err) => {
            log::debug!("{}", err);
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };
    // check the dst of the message is correct
    if message.destination.is_empty() || message.destination[0] != data.twin_id as u32{
        *response.status_mut() = StatusCode::BAD_REQUEST;
        return Ok(response);
    }


    let sender_twin = match data.twin_db.get_twin(message.source as u32).await {
        Ok(sender_twin) => sender_twin,
        Err(err) => {
            log::debug!("{}", err);
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };

    let verified = data.identity.verify(
        &message.signature.as_bytes(),
        &message.data.to_string(),
        "publickey".as_bytes(),
    );
    if verified == false {
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        return Ok(response);
    }
    //decode message

    Ok(Response::new("RmbRemote Endpoint".into()))
}

pub async fn rmb_reply<S: Storage, I: Identity, D: TwinDB>(
    _req: Request<Body>,
    _data: AppData<S, I, D>,
) -> Result<Response<Body>> {
    Ok(Response::new("RmbReply Endpoint".into()))
}

pub async fn routes<'a, S: Storage, I: Identity, D: TwinDB>(
    req: Request<Body>,
    data: AppData<S, I, D>,
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
