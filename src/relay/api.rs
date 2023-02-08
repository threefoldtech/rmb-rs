use crate::relay::throttler::Params;
use crate::token::{self, Claims};
use crate::twin::TwinDB;
use crate::types::Envelope;
use anyhow::{Context, Result};
use futures::stream::SplitSink;
use futures::Future;
use futures::{sink::SinkExt, stream::StreamExt};
use http::Method;
use hyper::service::Service;
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
use tokio::sync::Mutex;

use super::switch::{Hook, MessageID, StreamID, Switch};
use super::throttler::{Throttler, ThrottlerCache};
use super::HttpError;

pub(crate) struct AppData<D: TwinDB, T: ThrottlerCache> {
    switch: Arc<Switch<RelayHook>>,
    twins: D,
    domain: String,
    throttler: Arc<Throttler<T>>,
}

impl<D, T> AppData<D, T>
where
    D: TwinDB,
    T: ThrottlerCache,
{
    pub(crate) fn new<S: Into<String>>(
        domain: S,
        switch: Switch<RelayHook>,
        twins: D,
        throttler: Throttler<T>,
    ) -> Self {
        Self {
            domain: domain.into(),
            switch: Arc::new(switch),
            twins,
            throttler: Arc::new(throttler),
        }
    }
}
#[derive(Clone)]
pub(crate) struct HttpService<D: TwinDB, T: ThrottlerCache> {
    data: Arc<AppData<D, T>>,
}

impl<D, T> HttpService<D, T>
where
    D: TwinDB,
    T: ThrottlerCache,
{
    pub(crate) fn new(data: AppData<D, T>) -> Self {
        Self {
            data: Arc::new(data),
        }
    }
}

impl<D, T> Service<Request<Body>> for HttpService<D, T>
where
    D: TwinDB,
    T: ThrottlerCache,
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

async fn entry<D: TwinDB, T: ThrottlerCache>(
    data: Arc<AppData<D, T>>,
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

        // throttling should be done here

        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)?;

        // Spawn a task to handle the websocket connection.
        tokio::spawn(async move {
            if let Err(e) = serve_websocket(
                claims,
                Arc::clone(&data.switch),
                websocket,
                Arc::clone(&data.throttler),
            )
            .await
            {
                eprintln!("Error in websocket connection: {}", e);
            }
        });

        // Return the response so the spawned future can continue.
        return Ok(response);
    }

    // normal http handler
    match (request.method(), request.uri().path()) {
        (&Method::POST, "/") => federation(&data, request).await,
        (&Method::GET, "/metrics") => metrics(),
        _ => Err(HttpError::NotFound),
    }
}

fn metrics() -> Result<Response<Body>, HttpError> {
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

async fn federation<D: TwinDB, T: ThrottlerCache>(
    data: &AppData<D, T>,
    request: Request<Body>,
) -> Result<Response<Body>, HttpError> {
    // TODO:
    //   there are many things that can go wrong here
    //   - this is not an authorized endpoint. means anyone (not necessary a relay)
    //     can use to push messages to other twins. The receiver twin will fail to verify
    //     the signature (if the message is fake) but will produce a lot of traffic never the less
    //   - there is no check on the message size
    //   - there is no check if federation information is actually correct. we don't check
    //     if twin data on the chain (federation) is actually matching to this message for
    //     performance.
    //
    // this method trust whoever call it to provide correct federation information and proper message
    // size.
    let body = hyper::body::to_bytes(request.into_body())
        .await
        .map_err(|err| HttpError::BadRequest(err.to_string()))?;

    let envelope =
        Envelope::parse_from_bytes(&body).map_err(|err| HttpError::BadRequest(err.to_string()))?;

    match envelope.federation {
        Some(federation) if federation == data.domain => {
            // correct federation, accept the message
            let dst: StreamID = (&envelope.destination).into();
            data.switch.send(&dst, &body).await?;

            // check for federation information
            Response::builder()
                .status(http::StatusCode::ACCEPTED)
                .body(Body::empty())
                .map_err(HttpError::Http)
        }
        _ => Err(HttpError::BadRequest("invalid federation".into())),
    }
}

type Writer = SplitSink<WebSocketStream<Upgraded>, Message>;

pub(crate) struct RelayHook {
    peer: StreamID,
    switch: Arc<Switch<Self>>,
    writer: Arc<Mutex<Writer>>,
}

impl RelayHook {
    fn new(peer: StreamID, switch: Arc<Switch<Self>>, writer: Arc<Mutex<Writer>>) -> Self {
        Self {
            peer,
            switch,
            writer,
        }
    }
}

#[async_trait::async_trait]
impl Hook for RelayHook {
    async fn received<T>(&self, id: MessageID, data: T)
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

/// Handle a websocket connection.
async fn serve_websocket<T: ThrottlerCache>(
    claim: Claims,
    switch: Arc<Switch<RelayHook>>,
    websocket: HyperWebsocket,
    throttler: Arc<Throttler<T>>,
) -> Result<()> {
    let websocket = websocket.await?;
    let (writer, mut reader) = websocket.split();
    let writer_arc = Arc::new(Mutex::new(writer));
    // handler is kept alive to keep the registration alive
    // once dropped (connection closed) registration stops
    let id: StreamID = (claim.id, claim.sid).into();
    log::debug!("got connection from '{}'", id);
    let mut registration = switch
        .register(
            id.clone(),
            RelayHook::new(id, Arc::clone(&switch), writer_arc.clone()),
        )
        .await?;

    loop {
        tokio::select! {
            // registration cancelled. will be triggered
            // if the same twin with the same sid (session id)
            // registered again.
            // we need to drop the connection then
            _ = registration.cancelled() => {
                return Ok(());
            }
            // received a message from the peer
            Some(message) = reader.next()=> {
                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        log::debug!("error receiving a message: {}", err);
                        return Ok(());
                    }
                };

                // TODO: throttling to avoid sending too many messages in short time!
                match message {
                    Message::Text(_) => {
                        log::trace!("received unsupported (text) message. disconnecting!");
                        break;
                    }
                    Message::Binary(msg) => {
                        let envelope =
                        Envelope::parse_from_bytes(&msg).context("failed to load input message")?;
                        let params = Params{
                            size: msg.len(),
                            timestamp: envelope.timestamp,
                        };
                        if !throttler.can_send_message(&claim.id, &params).await? {
                            log::warn!("twin {} exceeded its rate limits, dropping message", claim.id);
                            let mut writer_lock = writer_arc.lock().await;
                            writer_lock.send(Message::Text(String::from("exceeded rate limits. message dropped"))).await?;
                            drop(writer_lock);
                            continue;

                        }
                        throttler.cache_message(&claim.id, &params).await?;
                        // TODO: federation
                        //  instead of sending the message directly to the switch
                        //  we check federation information attached to the envelope
                        //  if federation is not empty and does not match the domain
                        //  of this server, we need to schedule this message to be
                        //  federated to the right relay (according to specs)
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
