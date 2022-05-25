use super::data::AppData;
use crate::twin::{SubstrateTwinDB, TwinDB};
use crate::{cache::RedisCache, identity::Identity, storage::Storage, types::Message};
use anyhow::{Context, Result};
use hyper::http::{Method, Request, Response, Result as HTTPResult, StatusCode};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Server,
};
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

fn create_response(content: &str, status: StatusCode) {}

pub async fn rmb_remote<S: Storage, I: Identity, D: TwinDB>(
    req: Request<Body>,
    data: AppData<S, I, D>,
) -> HTTPResult<Response<(Body)>> {
    let mut response = Response::new(Body::empty());
    // getting request body
    let body = hyper::body::to_bytes(req.into_body()).await;
    let body = match body {
        Ok(body) => body,
        Err(err) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Failure in body"))
        }
    };
    let message: Result<Message> = serde_json::from_slice(&body.to_vec())
        .map_err(|err| anyhow::anyhow!(err))
        .context("can not parse body");

    let mut message = match message {
        Ok(message) => message,
        Err(err) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Failure in body: Couldn't parse message"))
        }
    };
    // check the dst of the message is correct
    if message.destination.is_empty() || message.destination[0] != data.twin_id as u32 {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("Failure in body: bad destination"));
    }
    // getting sender twin
    let sender_twin = match data.twin_db.get_twin(message.source as u32).await {
        Ok(sender_twin) => sender_twin,
        Err(err) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Couldn't find twin matching message source"))
        }
    };
    let sender_twin = match sender_twin {
        Some(sender_twin) => sender_twin,
        None => return Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from("Couldn't find twin matching message source")),
    };
    //verify the message
    let verified = data.identity.verify(
        message.signature.as_bytes().into(),
        &message.data.to_string(),
        sender_twin.account,
    );
    if verified == false {
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        return Ok(response);
    }

    message.reply = String::from("msgbus.system.reply");
    data.storage.run(message);
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(Body::from(""))
}

pub async fn rmb_reply<S: Storage, I: Identity, D: TwinDB>(
    _req: Request<Body>,
    _data: AppData<S, I, D>,
) -> HTTPResult<Response<Body>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("RMB Reply endpoint"))
}

pub async fn routes<'a, S: Storage, I: Identity, D: TwinDB>(
    req: Request<Body>,
    data: AppData<S, I, D>,
) -> HTTPResult<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/rmb-remote") => rmb_remote(req, data).await,
        (&Method::POST, "/rmb-reply") => rmb_reply(req, data).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()),
    }
}
