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
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::federation::Federator;
use super::limiter::{Metrics, RateLimiter};
use super::switch::{Hook, MessageID, StreamID, Switch};
use super::HttpError;

pub(crate) struct AppData<D: TwinDB, R: RateLimiter> {
    switch: Arc<Switch<RelayHook>>,
    twins: D,
    domain: String,
    federator: Arc<Federator>,
    limiter: R,
}

impl<D, R> AppData<D, R>
where
    D: TwinDB,
    R: RateLimiter,
{
    pub(crate) fn new<S: Into<String>>(
        domain: S,
        switch: Arc<Switch<RelayHook>>,
        twins: D,
        federator: Federator,
        limiter: R,
    ) -> Self {
        Self {
            domain: domain.into(),
            switch,
            twins,
            federator: Arc::new(federator),
            limiter,
        }
    }
}
#[derive(Clone)]

pub(crate) struct HttpService<D: TwinDB, R: RateLimiter> {
    data: Arc<AppData<D, R>>,
}

impl<D, R> HttpService<D, R>
where
    D: TwinDB,
    R: RateLimiter,
{
    pub(crate) fn new(data: AppData<D, R>) -> Self {
        Self {
            data: Arc::new(data),
        }
    }
}

impl<D, R> Service<Request<Body>> for HttpService<D, R>
where
    D: TwinDB,
    R: RateLimiter,
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

async fn entry<D: TwinDB, R: RateLimiter>(
    data: Arc<AppData<D, R>>,
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

        let metrics = data.limiter.get(twin.id).await;
        // Spawn a task to handle the websocket connection.
        let stream = Stream::new(
            // todo: improve the domain clone
            claims,
            data.domain.clone(),
            Arc::clone(&data.switch),
            Arc::clone(&data.federator),
            metrics,
        );

        tokio::spawn(async move {
            if let Err(err) = stream.serve(websocket).await {
                log::error!("error in websocket connection: {}", err);
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

async fn federation<D: TwinDB, R: RateLimiter>(
    data: &AppData<D, R>,
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

struct Stream<M: Metrics> {
    id: StreamID,
    domain: String,
    switch: Arc<Switch<RelayHook>>,
    federator: Arc<Federator>,
    metrics: M,
}
impl<M: Metrics> Stream<M> {
    fn new(
        claims: Claims,
        domain: String,
        switch: Arc<Switch<RelayHook>>,
        federator: Arc<Federator>,
        metrics: M,
    ) -> Self {
        let id: StreamID = (claims.id, claims.sid).into();
        Self {
            id,
            domain,
            switch,
            federator,
            metrics,
        }
    }

    async fn route(&self, envelope: &Envelope, msg: Vec<u8>) -> Result<()> {
        let dst: StreamID = (&envelope.destination).into();
        if self.id != envelope.source || dst.zero() {
            anyhow::bail!("message with missing source or destination");
        }

        //  instead of sending the message directly to the switch
        //  we check federation information attached to the envelope
        //  if federation is not empty and does not match the domain
        //  of this server, we need to schedule this message to be
        //  federated to the right relay (according to specs)
        if matches!(&envelope.federation, Some(fed) if fed != &self.domain) {
            // push message to the (relay.federation) queue
            return Ok(self.federator.send(&msg).await?);
        }

        // we don't return an error because when we return an error
        // we will send this error back to the sender user. Hence
        // calling the switch.send again
        // this is an internal error anyway, and should not happen
        if let Err(err) = self.switch.send(&dst, &msg).await {
            log::error!("failed to route message to peer '{}': {}", dst, err);
        }

        Ok(())
    }

    async fn serve(self, websocket: HyperWebsocket) -> Result<()> {
        let websocket = websocket.await?;
        let (writer, mut reader) = websocket.split();

        // handler is kept alive to keep the registration alive
        // once dropped (connection closed) registration stops
        log::debug!("got connection from '{}'", self.id);
        let mut registration = self
            .switch
            .register(
                self.id.clone(),
                RelayHook::new(self.id.clone(), Arc::clone(&self.switch), writer),
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
                            // failure to load the message is fatal hence we disconnect
                            // the client
                            let envelope =
                            Envelope::parse_from_bytes(&msg).context("failed to load input message")?;

                            #[cfg(feature = "tracker")]
                            super::switch::MESSAGE_RX_TWIN.with_label_values(&[&format!("{}", envelope.source.twin)]).inc();

                            if !self.metrics.measure(msg.len()).await {
                                log::trace!("twin with stream id {} exceeded its request limits, dropping message", self.id);
                                self.send_error(envelope, "exceeded rate limits, dropping message").await;
                                continue
                            }

                            // if we failed to route back the message to the user
                            // for any reason we send an error message back
                            // server error has no source address set.
                            if let Err(err) = self.route(&envelope, msg).await {
                                self.send_error(envelope, err).await;
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

    async fn send_error<E: Display>(&self, envelope: Envelope, err: E) {
        let mut resp = Envelope::new();
        resp.uid = envelope.uid;
        resp.destination = Some((&self.id).into()).into();
        let mut e = resp.mut_error();
        e.message = err.to_string();

        let bytes = match resp.write_to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                // this should never happen, hence let's print this as an error
                log::error!("failed to serialize envelope: {}", err);
                return;
            }
        };

        if let Err(err) = self.switch.send(&self.id, bytes).await {
            // just log then
            log::error!("failed to send error message back to caller: {}", err);
        }
    }
}
