use crate::token;
use crate::types::Envelope;
use anyhow::{Context, Result};
use futures::stream::SplitSink;
use futures::Future;
use futures::{sink::SinkExt, stream::StreamExt};
use http::Method;
use hyper::server::conn::Http;
use hyper::upgrade::Upgraded;
use hyper::Body;
use hyper::{Request, Response};
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

type Writer = SplitSink<WebSocketStream<Upgraded>, Message>;
struct RelayHook {
    peer: u32,
    switch: Arc<Switch<Self>>,
    writer: Arc<Mutex<Writer>>,
}

impl RelayHook {
    fn new(peer: u32, switch: Arc<Switch<Self>>, writer: Writer) -> Self {
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
        let mut writer = self.writer.lock().await;
        if let Err(err) = writer.send(Message::Binary(data.as_ref().into())).await {
            log::debug!("failed to forward message to peer: {}", err);
            return;
        }

        if let Err(err) = self.switch.ack(self.peer, &[id]).await {
            log::error!("failed to ack message ({}, {}): {}", self.peer, id, err);
        }
    }
}

pub struct Relay {
    switch: Switch<RelayHook>,
}

impl Relay {
    pub async fn new(opt: SwitchOptions) -> Result<Self> {
        let switch = opt.build().await?;
        Ok(Self { switch })
    }
    pub async fn start<A: ToSocketAddrs>(self, address: A) -> Result<()> {
        let tcp_listener = TcpListener::bind(address).await?;
        let http = HttpService {
            switch: Arc::new(self.switch),
        };

        loop {
            let (tcp_stream, _) = tcp_listener.accept().await?;
            let http = http.clone();
            tokio::task::spawn(async move {
                if let Err(http_err) = Http::new()
                    .http1_only(true)
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

async fn entry(
    switch: Arc<Switch<RelayHook>>,
    mut request: Request<Body>,
) -> Result<Response<Body>, http::Error> {
    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let jwt = match request.uri().query() {
            Some(token) => token,
            None => {
                log::debug!("missing jwt");
                return Response::builder()
                    .status(http::StatusCode::BAD_REQUEST)
                    .body(Body::from("missing jwt token"));
            }
        };

        let claims: token::Claims = match jwt.parse() {
            Ok(claims) => claims,
            Err(err) => {
                log::debug!("failed to parse claims: {}", err);
                return Response::builder()
                    .status(http::StatusCode::BAD_REQUEST)
                    .body(Body::from(err.to_string()));
            }
        };

        let (response, websocket) = match hyper_tungstenite::upgrade(&mut request, None) {
            Ok(v) => v,
            Err(err) => {
                return Response::builder()
                    .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(err.to_string()))
            }
        };

        // Spawn a task to handle the websocket connection.
        tokio::spawn(async move {
            if let Err(e) = serve_websocket(claims.id, switch, websocket).await {
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
                    .body(Body::from(err.to_string()));
            }

            Response::builder()
                .status(http::StatusCode::OK)
                .body(Body::from(buffer))
        }
        _ => Response::builder()
            .status(http::StatusCode::NOT_FOUND)
            .body(Body::empty()),
    }
}

/// Handle a websocket connection.
async fn serve_websocket(
    id: u32,
    switch: Arc<Switch<RelayHook>>,
    websocket: HyperWebsocket,
) -> Result<()> {
    let websocket = websocket.await?;
    let (writer, mut reader) = websocket.split();

    // handler is kept alive to keep the registration alive
    // once dropped (connection closed) registration stops
    let _handler = switch
        .register(id, RelayHook::new(id, Arc::clone(&switch), writer))
        .await?;

    while let Some(message) = reader.next().await {
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

                if let Err(err) = switch.send(envelope.destination, &msg).await {
                    log::error!(
                        "failed to route message to peer '{}': {}",
                        envelope.destination,
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

    Ok(())
}

use hyper::service::Service;

#[derive(Clone)]
struct HttpService {
    switch: Arc<Switch<RelayHook>>,
}

impl Service<Request<Body>> for HttpService {
    type Response = Response<Body>;
    type Error = http::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let fut = entry(Arc::clone(&self.switch), req);

        Box::pin(fut)
    }
}
