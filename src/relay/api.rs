use crate::token::{self, Claims};
use crate::twin::TwinDB;
use crate::types::{Envelope, Pong};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::stream::SplitSink;
use futures::Future;
use futures::{sink::SinkExt, stream::StreamExt};
use http::Method;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::Service;
use hyper::upgrade::Upgraded;
use hyper_tungstenite::tungstenite::Message;
use hyper_tungstenite::{HyperWebsocket, WebSocketStream};
use hyper_util::rt::TokioIo;
use prometheus::Encoder;
use prometheus::TextEncoder;
use protobuf::Message as ProtoMessage;
use std::collections::HashSet;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;

use super::federation::Federator;
use super::limiter::{Metrics, RateLimiter};
use super::switch::{ConnectionSender, MessageID, SendError, SessionID, Switch};
use super::HttpError;
use crate::relay::build_error_envelope;

pub(crate) struct AppData<D: TwinDB, R: RateLimiter> {
    switch: Arc<Switch<WriterCallback>>,
    twins: D,
    domains: HashSet<String>,
    federator: Arc<Federator>,
    limiter: R,
}

impl<D, R> AppData<D, R>
where
    D: TwinDB,
    R: RateLimiter,
{
    pub(crate) fn new(
        domains: HashSet<String>,
        switch: Arc<Switch<WriterCallback>>,
        twins: D,
        federator: Federator,
        limiter: R,
    ) -> Self {
        Self {
            domains,
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

impl<D, R> Service<Request<Incoming>> for HttpService<D, R>
where
    D: TwinDB,
    R: RateLimiter,
{
    type Response = Response<Full<Bytes>>;
    type Error = HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let data = Arc::clone(&self.data);

        let fut = async {
            match entry(data, req).await {
                Ok(result) => Ok(result),
                Err(err) => {
                    log::debug!("connection error: {:#}", err);
                    Response::builder()
                        .status(err.status())
                        .body(Full::from(Bytes::from(err.to_string())))
                        .map_err(HttpError::Http)
                }
            }
        };

        Box::pin(fut)
    }
}

async fn entry<D: TwinDB, R: RateLimiter>(
    data: Arc<AppData<D, R>>,
    mut request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, HttpError> {
    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let jwt = request.uri().query().ok_or(HttpError::MissingJWT)?;

        let claims: token::Claims = jwt.parse()?;

        let twin = data
            .twins
            .get_twin(claims.id.into())
            .await
            .map_err(|err| HttpError::FailedToGetTwin(err.to_string()))?
            .ok_or(HttpError::TwinNotFound(claims.id))?;

        token::verify(&twin.account, jwt)?;

        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)?;

        let metrics = data.limiter.get(twin.id).await;
        // Spawn a task to handle the websocket connection.
        let session = Session::new(
            // todo: improve the domain clone
            claims,
            data.domains.clone(),
            Arc::clone(&data.switch),
            Arc::clone(&data.federator),
            metrics,
            data.twins.clone(),
        );

        tokio::spawn(async move {
            if let Err(err) = session.serve(websocket).await {
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

fn metrics() -> Result<Response<Full<Bytes>>, HttpError> {
    // TODO add other end point
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    // Gather the metrics.
    let metric_families = prometheus::gather();
    // Encode them to send.
    if let Err(err) = encoder.encode(&metric_families, &mut buffer) {
        return Response::builder()
            .status(http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(Full::from(Bytes::from(err.to_string())))
            .map_err(HttpError::Http);
    }

    Response::builder()
        .status(http::StatusCode::OK)
        .body(Full::from(Bytes::from(buffer)))
        .map_err(HttpError::Http)
}

async fn federation<D: TwinDB, R: RateLimiter>(
    data: &AppData<D, R>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, HttpError> {
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
    let body = request
        .into_body()
        .collect()
        .await
        .map_err(|err| HttpError::BadRequest(err.to_string()))?
        .to_bytes();

    let envelope =
        Envelope::parse_from_bytes(&body).map_err(|err| HttpError::BadRequest(err.to_string()))?;

    let dst: SessionID = (&envelope.destination).into();

    // fast-fail path: if requested and destination is not local, return error immediately
    if has_fast_fail(&envelope) && !data.switch.is_local(&dst).await {
        // destination session doesn’t exist on this relay
        return Response::builder()
            .status(http::StatusCode::NOT_FOUND)
            .body(Full::from(Bytes::from("destination offline")))
            .map_err(HttpError::Http);
    }

    data.switch.send(&dst, &body).await?;

    Response::builder()
        .status(http::StatusCode::ACCEPTED)
        .body(Full::from(Bytes::new()))
        .map_err(HttpError::Http)
}

#[inline]
fn has_fast_fail(envelope: &Envelope) -> bool {
    envelope
        .tags
        .as_deref()
        .map(|t| t.contains("fast-fail"))
        .unwrap_or(false)
}

type Writer = SplitSink<WebSocketStream<TokioIo<Upgraded>>, Message>;

pub(crate) struct ConnectionWriter {
    peer: SessionID,
    cancellation: CancellationToken,
    writer: Writer,
    switch: Arc<Switch<WriterCallback>>,
}

impl ConnectionWriter {
    fn new(
        peer: SessionID,
        cancellation: CancellationToken,
        switch: Arc<Switch<WriterCallback>>,
        writer: Writer,
    ) -> Self {
        Self {
            peer,
            cancellation,
            switch,
            writer,
        }
    }

    fn start(self) -> WriterCallback {
        // TODO:
        // Regardless of the channel size. A slow connection can cause the messages to pile up
        // in the channel. Which will eventually causes the `[WriterCallback::handle]` to fail.
        // Hence it's important for the worker to tell the difference between a "Full" channel and a
        // "Closed" channel. And then react differently to the error. One suggesting if a channel
        // is full the worker can wait (on the background) until capacity is available for that client
        // or timesout. If timedout or suddenly the connection is closed, it should then cancel that
        // connection.
        let (tx, rx) = mpsc::channel(256);

        tokio::spawn(self.run(rx));

        WriterCallback { tx }
    }

    async fn run(mut self, mut rx: Receiver<(MessageID, Vec<u8>)>) {
        let drop_guard = self.cancellation.clone().drop_guard();
        // Ack batching params
        const ACK_BATCH_SIZE: usize = 128;
        const ACK_FLUSH_MS: u64 = 10;

        let mut pending_acks: Vec<MessageID> = Vec::with_capacity(ACK_BATCH_SIZE);
        let mut last_flush = Instant::now();

        while let Some(Some((id, data))) = self.cancellation.run_until_cancelled(rx.recv()).await {
            log::trace!("relaying message {} to peer {}", id, self.peer);
            match self
                .cancellation
                .run_until_cancelled(self.writer.send(Message::Binary(data)))
                .await
            {
                // cancelled; break so the loop exits and the final ACK flush runs.
                None => break,
                // failed to write
                Some(Err(err)) => {
                    _ = self.writer.close().await;
                    log::debug!("failed to forward message to peer: {}", err);
                    return;
                }
                // written
                Some(_) => {}
            }

            // Batch ACKs to reduce Redis round-trips
            pending_acks.push(id);
            let should_flush = pending_acks.len() >= ACK_BATCH_SIZE
                || last_flush.elapsed() >= Duration::from_millis(ACK_FLUSH_MS);
            if should_flush {
                if let Err(e) = self.switch.ack(&self.peer, &pending_acks).await {
                    log::debug!("ack batch failed for {} ids: {}", pending_acks.len(), e);
                }
                pending_acks.clear();
                last_flush = Instant::now();
            }
        }

        // Flush any remaining acks on exit
        if !pending_acks.is_empty() {
            if let Err(e) = self.switch.ack(&self.peer, &pending_acks).await {
                log::debug!(
                    "final ack batch failed for {} ids: {}",
                    pending_acks.len(),
                    e
                );
            }
        }

        drop(drop_guard);
    }
}

pub(crate) struct WriterCallback {
    tx: Sender<(MessageID, Vec<u8>)>,
}

impl ConnectionSender for WriterCallback {
    fn send(&mut self, id: MessageID, data: Vec<u8>) -> Result<(), SendError> {
        self.tx.try_send((id, data)).map_err(|err| match err {
            TrySendError::Closed(_) => SendError::Closed,
            TrySendError::Full(_) => SendError::NotEnoughCapacity,
        })
    }

    fn can_send(&self) -> bool {
        !self.tx.is_closed() && self.tx.capacity() > 0
    }
}

struct Session<M: Metrics, D: TwinDB> {
    id: SessionID,
    domains: HashSet<String>,
    switch: Arc<Switch<WriterCallback>>,
    federator: Arc<Federator>,
    metrics: M,
    twins: D,
}

impl<M: Metrics, D: TwinDB> Session<M, D> {
    fn new(
        claims: Claims,
        domains: HashSet<String>,
        switch: Arc<Switch<WriterCallback>>,
        federator: Arc<Federator>,
        metrics: M,
        twins: D,
    ) -> Self {
        let id = SessionID::new(claims.id.into(), claims.sid);
        Self {
            id,
            domains,
            switch,
            federator,
            metrics,
            twins,
        }
    }

    async fn route(&self, envelope: &mut Envelope, mut msg: Vec<u8>) -> Result<()> {
        if self.id != envelope.source {
            anyhow::bail!("message wrong source");
        }

        if envelope.has_ping() {
            log::trace!("got an envelope ping message");
            // if ping just change it to pong and send back
            envelope.set_pong(Pong::default());
            std::mem::swap(&mut envelope.source, &mut envelope.destination);
            let plain = envelope.mut_plain();
            plain.extend_from_slice("hello world".as_bytes());
            // override the serialized data
            msg = envelope.write_to_bytes()?;
        }

        let dst: SessionID = (&envelope.destination).into();
        if dst.is_empty() {
            anyhow::bail!("message with missing destination");
        }

        let is_local = self.switch.is_local(&dst).await;

        // check if the dst twin id is already connected locally
        // if so, we don't have to check federation and directly
        // switch the message
        if is_local {
            log::trace!("found local session for '{}' , forwarding message", dst);
            if let Err(err) = self.switch.send(&dst, &msg).await {
                log::error!("failed to route message to peer '{}' : {}", dst, err);
                anyhow::bail!("failed to route message to peer '{}': {}", dst, err);
            }
            return Ok(());
        }

        // if destination is not local, we need to check if we need to federate
        // or not.
        //  instead of sending the message directly to the switch
        //  we check federation information attached to the envelope
        //  if federation is not empty and does not match the domain
        //  of this server, we need to schedule this message to be
        //  federated to the right relay (according to specs)
        let twin = self
            .twins
            .get_twin(envelope.destination.twin.into())
            .await?
            .ok_or_else(|| anyhow::Error::msg("unknown twin destination"))?;

        if !twin
            .relay
            .ok_or_else(|| anyhow::Error::msg("relay info is not set for this twin"))?
            .has_common(&self.domains)
        {
            log::debug!("got an foreign message");
            // push message to the (relay.federation) queue
            return Ok(self.federator.send(&msg).await?);
        }
        // fast-fail path: we confirmed that this is not an foreign message, and if it is fast-fail taged and destination session is not connected
        // then the peer must be offline, return error immediately
        if has_fast_fail(envelope) && !is_local {
            anyhow::bail!("destination offline");
        }
        // we don't return an error because when we return an error
        // we will send this error back to the sender user. Hence
        // calling the switch.send again
        // this is an internal error anyway, and should not happen
        if let Err(err) = self.switch.send(&dst, &msg).await {
            log::error!("failed to route message to peer '{}': {}", dst, err);
            anyhow::bail!("failed to route message to peer '{}': {}", dst, err);
        }

        Ok(())
    }

    async fn serve(self, websocket: HyperWebsocket) -> Result<()> {
        let websocket = websocket.await?;
        let (writer, mut reader) = websocket.split();

        // handler is kept alive to keep the registration alive
        // once dropped (connection closed) registration stops
        log::debug!("got connection from '{}'", self.id);
        let cancellation = CancellationToken::new();

        self.switch
            .register(
                self.id.clone(),
                cancellation.clone(),
                ConnectionWriter::new(
                    self.id.clone(),
                    cancellation.clone(),
                    Arc::clone(&self.switch),
                    writer,
                )
                .start(),
            )
            .await?;

        let drop_guard = cancellation.clone().drop_guard();
        let mut cancelled = std::pin::pin!(cancellation.cancelled());

        loop {
            tokio::select! {
                // registration cancelled. will be triggered
                // if the same twin with the same sid (session id)
                // registered again.
                // we need to drop the connection then
                _ = &mut cancelled => {
                    break;
                }
                // received a message from the peer
                Some(message) = reader.next()=> {
                    let message = match message {
                        Ok(message) => message,
                        Err(err) => {
                            log::debug!("error receiving a message: {}", err);
                            break;
                        }
                    };

                    // TODO: throttling to avoid sending too many messages in short time!
                    match message {
                        Message::Text(_) => {
                            log::debug!("received unsupported (text) message. disconnecting!");
                            break;
                        }
                        Message::Binary(msg) => {
                            // failure to load the message is fatal hence we disconnect
                            // the client
                            let mut envelope =
                            Envelope::parse_from_bytes(&msg).context("failed to load input message")?;

                            #[cfg(feature = "tracker")]
                            super::switch::MESSAGE_RX_TWIN.with_label_values(&[&format!("{}", envelope.source.twin)]).inc();

                            if !self.metrics.measure(msg.len()).await {
                                log::debug!("twin with stream id {} exceeded its request limits, dropping message", self.id);
                                self.send_error_response(envelope, "exceeded rate limits, dropping message").await;
                                continue;
                            }

                            // if we failed to route back the message to the user
                            // for any reason we send an error message back
                            // server error has no source address set.
                            if let Err(err) = self.route(&mut envelope, msg).await {
                                self.send_error_response(envelope, err).await;
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

        self.switch.unregister(self.id).await;
        drop(drop_guard);
        Ok(())
    }

    async fn send_error_response<E: Display>(&self, envelope: Envelope, err: E) {
        if !envelope.has_request() {
            return;
        }
        let mut resp = build_error_envelope(&envelope, err);
        resp.destination = Some((&self.id).into()).into();

        match resp.write_to_bytes() {
            Ok(bytes) => {
                if let Err(err) = self.switch.send(&self.id, bytes).await {
                    // just log then
                    log::error!("failed to send error message back to caller: {}", err);
                }
            }
            Err(err) => {
                // this should never happen, hence let's print this as an error
                log::error!("failed to serialize envelope: {}", err);
            }
        }
    }
}
