use super::data::{AppData, UploadConfig};
use crate::twin::TwinDB;
use crate::types::UploadPayload;
use crate::{identity::Identity, storage::Storage, types::Message};
use anyhow::{Context, Result};
use futures::TryStreamExt;
use hyper::http::{header, Method, Request, Response, Result as HTTPResult, StatusCode};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Server,
};
use mpart_async::server::{MultipartField, MultipartStream};
use std::convert::Infallible;
use std::fs::{self, File};
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;

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
                upload_config: upload_config,
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
    #[error("bad request: {0:#}")]
    BadRequest(anyhow::Error),

    #[error("unauthorized: {0:#}")]
    UnAuthorized(anyhow::Error),

    #[error("invalid destination twin {0}")]
    InvalidDestination(u32),

    #[error("invalid source twin {0}: {1:#}")]
    InvalidSource(u32, anyhow::Error),

    #[error("internal server error: {0:#}")]
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
        .verify(sender_twin.account)
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

fn get_header(request: &Request<Body>, key: &str) -> String {
    match request.headers().get(key).and_then(|val| val.to_str().ok()) {
        Some(value) => value.to_string(),
        None => "".to_string(),
    }
}

fn verify_upload_request<S: Storage, I: Identity, D: TwinDB>(
    data: &AppData<S, I, D>,
    request: &Request<Body>,
) -> Result<UploadPayload> {
    let source = get_header(&request, "rmb-source-id").parse::<u32>()?;
    let timestamp = get_header(&request, "rmb-timestamp").parse::<u64>()?;

    let payload = UploadPayload::new(
        "".to_string(),
        get_header(&request, "rmb-upload-cmd"),
        source,
        timestamp,
        get_header(&request, "rmb-signature"),
    );

    payload.verify(&data.identity)?;
    Ok(payload)
}

async fn send_process_upload_message<S: Storage, I: Identity, D: TwinDB>(
    data: &AppData<S, I, D>,
    payload: &UploadPayload,
) {
    let mut msg = Message::default();

    let dst = vec![payload.source];
    msg.source = data.twin;
    msg.destination = dst;
    msg.command = payload.cmd.clone();
    msg.data = base64::encode(&payload.path);
    msg.stamp();

    log::debug!("sending to upload command: {}", msg.command);

    if let Err(err) = data
        .storage
        .run(msg)
        .await
        .context("can not run upload command")
    {
        log::error!("failed to run upload command: {}", err);
    }
}

fn get_multipart_stream<'a>(
    request: &'a mut Request<Body>,
) -> Result<MultipartStream<&'a mut Body, hyper::Error>> {
    let m = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|val| val.to_str().ok())
        .and_then(|val| val.parse::<mime::Mime>().ok())
        .ok_or_else(|| anyhow!("cannot get mime type"))?;

    let boundary = m
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| anyhow!("cannot get content boundary"))?;

    let body = request.body_mut();
    let stream = MultipartStream::new(boundary, body);

    Ok(stream)
}

async fn process_multipart_field<'a, S: Storage, I: Identity, D: TwinDB>(
    data: &AppData<S, I, D>,
    payload: &mut UploadPayload,
    field: &mut MultipartField<&'a mut Body, hyper::Error>,
) -> Result<()> {
    log::debug!("Field received:{}", field.name().unwrap_or_default());
    if let Ok(filename) = field.filename() {
        log::debug!("Field filename:{}", filename);

        let filename = Path::new(filename.as_ref())
            .file_name()
            .ok_or_else(|| anyhow!("file name is not valid"))?;

        let parent_dir =
            Path::new(&data.upload_config.files_path).join(format!("{}", uuid::Uuid::new_v4()));

        fs::create_dir_all(&parent_dir).with_context(|| "cannot create the parent directory")?;

        let path_buf = parent_dir.join(&filename);
        // to make it consistent between here and the processor
        let path = path_buf.to_string_lossy();
        payload.path = path.to_string();
        let mut file = File::create(&payload.path).with_context(|| "cannot create the file")?;
        while let Ok(Some(bytes)) = field.try_next().await {
            file.write_all(&bytes)
                .with_context(|| "cannot write data to file")?;
        }
    }

    Ok(())
}

async fn rmb_upload_handler<S: Storage, I: Identity, D: TwinDB>(
    mut request: Request<Body>,
    data: AppData<S, I, D>,
) -> Result<(), HandlerError> {
    // first verify this upload request
    let mut payload = match verify_upload_request(&data, &request) {
        Ok(p) => p,
        Err(err) => return Err(HandlerError::BadRequest(err)),
    };

    let mut stream = match get_multipart_stream(&mut request) {
        Ok(s) => s,
        Err(err) => return Err(HandlerError::BadRequest(err)),
    };

    while let Ok(Some(mut field)) = stream.try_next().await {
        if let Err(err) = process_multipart_field(&data, &mut payload, &mut field).await {
            log::debug!("error processing multipart field: {}", err.to_string());
            return Err(HandlerError::InternalError(err));
        } else {
            send_process_upload_message(&data, &payload).await;
        }
    }

    Ok(())
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

    use std::time::SystemTime;

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
                files_path: "tmp".to_string(),
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
                files_path: "tmp".to_string(),
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
