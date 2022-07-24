use super::data::AppData;
use super::errors::HandlerError;
use super::upload::{UploadConfig, UploadHandler};
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
use std::time::Duration;

const MAX_AGE: Duration = Duration::from_secs(60);

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
        upload_config: UploadConfig,
    ) -> Result<Self> {
        let addr: SocketAddr = listen.as_ref().parse().context("failed to parse address")?;
        Ok(HttpApi {
            addr,
            data: AppData {
                twin,
                storage,
                identity,
                twin_db,
                upload_config,
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

    let message: Message = serde_json::from_slice(&body)
        .context("failed to parse message")
        .map_err(HandlerError::BadRequest)?;

    // we need to also check the message age to make sure
    // it's not a 'reply'
    if message.age() > MAX_AGE {
        return Err(HandlerError::BadRequest(anyhow!("message is too old")));
    }

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

    message
        .valid()
        .context("message validation failed")
        .map_err(HandlerError::BadRequest)?;

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
        .run(message)
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
        Err(error) => {
            log::debug!("failed to handle request message: {}", error);
            Response::builder()
                .status(error.code())
                .body(Body::from(error.to_string()))
        }
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

    // reset the values back to original
    // as per the source.
    message.reply = source.reply;
    message.tag = source.tag;

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
        Err(err) => {
            log::error!("failed to handle reply message: {}", err);
            Response::builder()
                .status(err.code())
                .body(Body::from(err.to_string()))
        }
    }
}

async fn rmb_upload_handler<S: Storage, I: Identity, D: TwinDB>(
    request: Request<Body>,
    data: AppData<S, I, D>,
) -> Result<(), HandlerError> {
    let handler = UploadHandler::new(data);

    handler.handle(request).await
}

pub async fn rmb_upload<S: Storage, I: Identity, D: TwinDB>(
    request: Request<Body>,
    data: AppData<S, I, D>,
) -> HTTPResult<Response<Body>> {
    // only if uploads are enabled
    if !data.upload_config.enabled {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty());
    }

    match rmb_upload_handler(request, data).await {
        Ok(_) => Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty()),
        Err(err) => {
            log::error!("failed to handle upload: {}", err);
            Response::builder()
                .status(err.code())
                .body(Body::from(err.to_string()))
        }
    }
}

pub async fn routes<'a, S: Storage, I: Identity, D: TwinDB>(
    req: Request<Body>,
    data: AppData<S, I, D>,
) -> HTTPResult<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/zbus-remote") => rmb_remote(req, data).await,
        (&Method::POST, "/zbus-reply") => rmb_reply(req, data).await,
        (&Method::POST, "/zbus-upload") => rmb_upload(req, data).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()),
    }
}

#[cfg(test)]
mod tests {

    use std::{path::PathBuf, time::SystemTime};

    use crate::http_api::mock::{Identities, StorageMock, TwinDBMock};

    use super::*;
    use http::Request;

    #[tokio::test]
    async fn test_rmb_remote() {
        // In this test account with twin_id=1 sends to account with twin_id=2
        let twin_db = TwinDBMock;
        let storage = StorageMock;

        let twin: u32 = 2;

        let data = AppData {
            twin,
            storage,
            identity: Identities::get_recv_identity(),
            twin_db,
            upload_config: UploadConfig {
                enabled: true,
                upload_dir: PathBuf::from("/tmp"),
            },
        };

        let req = Request::builder()
            .uri("http://codescalers.com/rmb-remote")
            .method(Method::POST)
            .header("content-type", "application/json");

        let mut msg = Message::default();
        msg.source = 1;
        msg.destination = vec![2];
        msg.data = String::from("dsads");
        msg.timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        msg.expiration = 300;
        msg.sign(&Identities::get_sender_identity());

        let request = req
            .body(Body::from(serde_json::to_vec(&msg).unwrap()))
            .with_context(|| format!("can not construct request body for this id '{}'", twin))
            .unwrap();
        let response = rmb_remote(request, data).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
    #[tokio::test]
    async fn test_rmb_remote_unauthorized() {
        // In this test account with twin_id=1 sends to account with twin_id=2
        let twin_db = TwinDBMock;
        let storage = StorageMock;

        let twin: u32 = 2;

        let data = AppData {
            twin,
            storage,
            identity: Identities::get_recv_identity(),
            twin_db,
            upload_config: UploadConfig {
                enabled: true,
                upload_dir: PathBuf::from("/tmp"),
            },
        };

        let req = Request::builder()
            .uri("http://codescalers.com/rmb-remote")
            .method(Method::POST)
            .header("content-type", "application/json");

        let mut msg = Message::default();
        msg.source = 3;
        msg.destination = vec![2];
        msg.data = String::from("message data");
        msg.timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        msg.expiration = 10;
        msg.sign(&Identities::get_sender_identity());

        let request = req
            .body(Body::from(serde_json::to_vec(&msg).unwrap()))
            .with_context(|| format!("can not construct request body for this id '{}'", twin))
            .unwrap();
        let response = rmb_remote(request, data).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
