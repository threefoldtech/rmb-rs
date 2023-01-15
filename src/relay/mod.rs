use crate::token::{self, Claims};
use crate::twin::TwinDB;
use crate::types::Envelope;
use anyhow::{Context, Result};
use futures::stream::SplitSink;
use futures::Future;
use futures::{sink::SinkExt, stream::StreamExt};
use http::Method;
use hyper::server::conn::Http;
use hyper::service::Service;
use hyper::upgrade::Upgraded;
use hyper::Body;
use hyper::{Request, Response};
use hyper_tungstenite::tungstenite::error::ProtocolError;
use hyper_tungstenite::tungstenite::Message;
use hyper_tungstenite::{HyperWebsocket, WebSocketStream};
use prometheus::Encoder;
use prometheus::TextEncoder;
use protobuf::Message as ProtoMessage;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::net::ToSocketAddrs;
use tokio::sync::Mutex;

mod switch;
pub use switch::SwitchOptions;
use switch::{Hook, Switch};

use self::switch::StreamID;

type Writer = SplitSink<WebSocketStream<Upgraded>, Message>;
struct RelayHook {
    peer: StreamID,
    switch: Arc<Switch<Self>>,
    writer: Arc<Mutex<Writer>>,
}

impl RelayHook {
    fn new(peer: StreamID, switch: Arc<Switch<Self>>, writer: Writer) -> Self {
        Self {
            peer,
            switch,
            writer: Arc::new(Mutex::new(writer)),
        }
    }
}

#[async_trait::async_trait]
impl Hook for RelayHook {
    async fn received<T>(&self, id: switch::MessageID, data: T)
    where
        T: AsRef<[u8]> + Send + Sync,
    {
        log::trace!("relaying message {} to peer {}", id, self.peer);
        let mut writer = self.writer.lock().await;
        if let Err(err) = writer.send(Message::Binary(data.as_ref().into())).await {
            log::debug!("failed to forward message to peer: {}", err);
            return;
        }

        if let Err(err) = self.switch.ack(&self.peer, &[id]).await {
            log::error!("failed to ack message ({}, {}): {}", self.peer, id, err);
        }
    }
}

pub struct Relay<D: TwinDB> {
    switch: Switch<RelayHook>,
    twins: D,
}

impl<D> Relay<D>
where
    D: TwinDB + Clone,
{
    pub async fn new(twins: D, opt: SwitchOptions) -> Result<Self> {
        let switch = opt.build().await?;
        Ok(Self { switch, twins })
    }

    pub async fn start<A: ToSocketAddrs>(self, address: A) -> Result<()> {
        let tcp_listener = TcpListener::bind(address).await?;
        let data = AppData {
            switch: Arc::new(self.switch),
            twins: self.twins,
        };

        let http = HttpService {
            data: Arc::new(data),
        };

        loop {
            let (tcp_stream, _) = tcp_listener.accept().await?;
            let http = http.clone();
            tokio::task::spawn(async move {
                if let Err(http_err) = Http::new()
                    .http1_keep_alive(true)
                    .serve_connection(tcp_stream, http)
                    .with_upgrades()
                    .await
                {
                    eprintln!("Error while serving HTTP connection: {}", http_err);
                }
            });
        }
    }
}

struct AppData<D: TwinDB> {
    switch: Arc<Switch<RelayHook>>,
    twins: D,
}

#[derive(thiserror::Error, Debug)]
enum HttpError {
    #[error("missing jwt")]
    MissingJWT,
    #[error("invalid jwt: {0}")]
    InvalidJWT(#[from] token::Error),
    #[error("failed to get twin: {0}")]
    FailedToGetTwin(String),
    #[error("twin not found {0}")]
    TwinNotFound(u32),
    #[error("{0}")]
    WebsocketError(#[from] ProtocolError),
    #[error("page not found")]
    NotFound,
    // generic catch all
    #[error("{0}s")]
    Http(#[from] http::Error),
}

impl HttpError {
    pub fn status(&self) -> http::StatusCode {
        use http::StatusCode as Codes;
        match self {
            Self::MissingJWT => Codes::BAD_REQUEST,
            Self::InvalidJWT(_) => Codes::UNAUTHORIZED,
            Self::FailedToGetTwin(_) => Codes::INTERNAL_SERVER_ERROR,
            Self::TwinNotFound(_) => Codes::UNAUTHORIZED,
            Self::WebsocketError(_) => Codes::INTERNAL_SERVER_ERROR,
            Self::NotFound => Codes::NOT_FOUND,
            Self::Http(_) => Codes::INTERNAL_SERVER_ERROR,
        }
    }
}
#[derive(Clone)]
struct HttpService<D: TwinDB> {
    data: Arc<AppData<D>>,
}

impl<D> Service<Request<Body>> for HttpService<D>
where
    D: TwinDB,
{
    type Response = Response<Body>;
    type Error = HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let data = Arc::clone(&self.data);

        let fut = async {
            match entry(data, req).await {
                Ok(result) => Ok(result),
                Err(err) => {
                    log::debug!("connection error: {:#}", err);
                    Response::builder()
                        .status(err.status())
                        .body(Body::from(err.to_string()))
                        .map_err(HttpError::Http)
                }
            }
        };

        Box::pin(fut)
    }
}

async fn entry<D: TwinDB>(
    data: Arc<AppData<D>>,
    mut request: Request<Body>,
) -> Result<Response<Body>, HttpError> {
    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let jwt = request.uri().query().ok_or(HttpError::MissingJWT)?;

        let claims: token::Claims = jwt.parse()?;

        let twin = data
            .twins
            .get_twin(claims.id)
            .await
            .map_err(|err| HttpError::FailedToGetTwin(err.to_string()))?
            .ok_or(HttpError::TwinNotFound(claims.id))?;

        token::verify(&twin.account, jwt)?;

        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)?;

        // Spawn a task to handle the websocket connection.
        tokio::spawn(async move {
            if let Err(e) = serve_websocket(claims, Arc::clone(&data.switch), websocket).await {
                eprintln!("Error in websocket connection: {}", e);
            }
        });

        // Return the response so the spawned future can continue.
        return Ok(response);
    }

    // normal http handler
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => {
            // TODO add other end point
            let mut buffer = Vec::new();
            let encoder = TextEncoder::new();

            // Gather the metrics.
            let metric_families = prometheus::gather();
            // Encode them to send.
            if let Err(err) = encoder.encode(&metric_families, &mut buffer) {
                return Response::builder()
                    .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(err.to_string()))
                    .map_err(HttpError::Http);
            }

            Response::builder()
                .status(http::StatusCode::OK)
                .body(Body::from(buffer))
                .map_err(HttpError::Http)
        }
        _ => Err(HttpError::NotFound),
    }
}

/// Handle a websocket connection.
async fn serve_websocket(
    claim: Claims,
    switch: Arc<Switch<RelayHook>>,
    websocket: HyperWebsocket,
) -> Result<()> {
    let websocket = websocket.await?;
    let (writer, mut reader) = websocket.split();

    // handler is kept alive to keep the registration alive
    // once dropped (connection closed) registration stops
    let id: StreamID = (claim.id, claim.sid).into();
    log::debug!("got connection from '{}'", id);
    let mut registration = switch
        .register(id.clone(), RelayHook::new(id, Arc::clone(&switch), writer))
        .await?;

    // todo: if the same twin connected twice to the relay the switch will maintain a single registration
    // to that identity. Hence, while this connection will remain open (and receiving messages) all
    // messages received for this "twin" will be sent back over the new connection alone
    //
    // 2 solutions here:
    //  - somehow this loop is notified that registration has been terminated
    //    some sort of select!{handler, read}
    //  - or allow fanning out, basically maintain multiple registration of the
    //    same identity (twin)

    loop {
        tokio::select! {
            _ = registration.cancelled() => {
                return Ok(());
            }
            Some(message) = reader.next()=> {
                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        log::debug!("error receiving a message: {}", err);
                        return Ok(());
                    }
                };

                match message {
                    Message::Text(_) => {
                        log::trace!("received unsupported (text) message. disconnecting!");
                        break;
                    }
                    Message::Binary(msg) => {
                        let envelope =
                            Envelope::parse_from_bytes(&msg).context("failed to load input message")?;

                        let dst: StreamID = (&envelope.destination).into();
                        if let Err(err) = switch.send(&dst, &msg).await {
                            log::error!(
                                "failed to route message to peer '{}': {}",
                                dst,
                                err
                            );
                        }
                    }
                    Message::Ping(_) => {
                        // No need to send a reply: tungstenite takes care of this for you.
                    }
                    Message::Pong(_) => {
                        log::trace!("received pong message");
                    }
                    Message::Close(_) => {
                        break;
                    }
                    Message::Frame(_) => {
                        unreachable!();
                    }
                }
            }
        }
    }

    Ok(())
}
