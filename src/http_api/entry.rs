use super::data::AppData;
use crate::twin::TwinDB;
use crate::{identity::Identity, storage::Storage, types::Message};
use anyhow::{Context, Result};
use hyper::http::{Method, Request, Response, Result as HTTPResult, StatusCode};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Server,
};
use std::convert::Infallible;
use std::net::SocketAddr;
use thiserror::Error;
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
        twin: u32,
        listen: P,
        storage: S,
        identity: I,
        twin_db: D,
    ) -> Result<Self> {
        let addr: SocketAddr = listen.as_ref().parse().context("failed to parse address")?;
        Ok(HttpApi {
            addr,
            data: AppData {
                twin,
                storage,
                identity,
                twin_db,
            },
        })
    }

    pub async fn run(self) -> Result<()> {
        let services = make_service_fn(move |_| {
            let data = self.data.clone();
            let service = service_fn(move |req| routes(req, data.clone()));
            async move { Ok::<_, Infallible>(service) }
        });

        let server = Server::try_bind(&self.addr)?.serve(services);
        log::info!("listening on: {}", self.addr);

        server.await?;

        Ok(())
    }
}
#[derive(Error, Debug)]
enum HandlerError {
    #[error("bad request: {0}")]
    BadRequest(anyhow::Error),

    #[error("un authorized: {0}")]
    UnAuthorized(anyhow::Error),

    #[error("invalid destination twin {0}")]
    InvalidDestination(u32),

    #[error("invalid source twin {0}: {1}")]
    InvalidSource(u32, anyhow::Error),

    #[error("internal server error")]
    InternalError(#[from] anyhow::Error),
}

impl HandlerError {
    fn code(&self) -> StatusCode {
        match self {
            // the following ones are considered a bad request error
            HandlerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            HandlerError::InvalidDestination(_) => StatusCode::BAD_REQUEST,

            // Unauthorized errors
            HandlerError::UnAuthorized(_) => StatusCode::UNAUTHORIZED,
            HandlerError::InvalidSource(_, _) => StatusCode::UNAUTHORIZED,

            // Internal server error
            HandlerError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

async fn message<S: Storage, I: Identity, D: TwinDB>(
    request: Request<Body>,
    data: &AppData<S, I, D>,
) -> Result<Message, HandlerError> {
    // getting request body
    let body = hyper::body::to_bytes(request.into_body())
        .await
        .context("failed to read request body")
        .map_err(HandlerError::BadRequest)?;

    let mut message: Message = serde_json::from_slice(&body.to_vec())
        .context("failed to parse message")
        .map_err(HandlerError::BadRequest)?;

    // check the dst of the message is correct
    if message.destination.is_empty() || message.destination[0] != data.twin as u32 {
        return Err(HandlerError::InvalidDestination(message.destination[0]));
    }

    // getting sender twin
    let sender_twin = data
        .twin_db
        .get_twin(message.source as u32)
        .await
        .context("failed to get source twin")?;

    let sender_twin = match sender_twin {
        Some(twin) => twin,
        None => {
            return Err(HandlerError::InvalidSource(
                message.source,
                anyhow::anyhow!("source twin not found"),
            ))
        }
    };

    //verify the message
    message
        .verify(&sender_twin.account)
        .map_err(HandlerError::UnAuthorized)?;

    Ok(message)
}

async fn rmb_remote_handler<S: Storage, I: Identity, D: TwinDB>(
    request: Request<Body>,
    data: AppData<S, I, D>,
) -> Result<(), HandlerError> {
    let message = message(request, &data).await?;

    data.storage
        .run(&message)
        .await
        .map_err(HandlerError::InternalError)
}

pub async fn rmb_remote<S: Storage, I: Identity, D: TwinDB>(
    request: Request<Body>,
    data: AppData<S, I, D>,
) -> HTTPResult<Response<Body>> {
    match rmb_remote_handler(request, data).await {
        Ok(_) => Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty()),
        Err(error) => Response::builder()
            .status(error.code())
            .body(Body::from(error.to_string())),
    }
}

async fn rmb_reply_handler<S: Storage, I: Identity, D: TwinDB>(
    request: Request<Body>,
    data: AppData<S, I, D>,
) -> Result<(), HandlerError> {
    let mut message = message(request, &data).await?;
    let source = data
        .storage
        .get(&message.id)
        .await
        .context("failed to get source message")?;

    let source = match source {
        Some(source) => source,
        None => {
            // if source message is now none, it means it probably
            // has timed out. so we just drop it
            return Ok(());
        }
    };

    message.reply = source.reply;

    data.storage
        .reply(&message)
        .await
        .map_err(HandlerError::InternalError)
}

pub async fn rmb_reply<S: Storage, I: Identity, D: TwinDB>(
    request: Request<Body>,
    data: AppData<S, I, D>,
) -> HTTPResult<Response<Body>> {
    match rmb_reply_handler(request, data).await {
        Ok(_) => Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty()),
        Err(error) => Response::builder()
            .status(error.code())
            .body(Body::from(error.to_string())),
    }
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
