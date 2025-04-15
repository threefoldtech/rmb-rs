use crate::token::{self, Claims};
use crate::twin::{RelayDomains, TwinDB};
use crate::types::{Envelope, EnvelopeExt, Pong};
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
use std::collections::HashSet;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;

use super::federation::Federator;
use super::limiter::{Metrics, RateLimiter};
use super::switch::{Callback, CallbackError, MessageID, SessionID, Switch};
use super::HttpError;

pub(crate) struct AppData<D: TwinDB, R: RateLimiter> {
    switch: Arc<Switch<WriterCallback>>,
    twins: D,
    domain: HashSet<String>,
    federator: Arc<Federator>,
    limiter: R,
}

impl<D, R> AppData<D, R>
where
    D: TwinDB,
    R: RateLimiter,
{
    pub(crate) fn new(
        domain: HashSet<String>,
        switch: Arc<Switch<WriterCallback>>,
        twins: D,
        federator: Federator,
        limiter: R,
    ) -> Self {
        Self {
            domain: domain,
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
            data.domain.clone(),
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

    // todo: this is UNSAFE. since we can't verify the received message hence
    // we shouldn't poison local cache

    update_cache_relays(&envelope, &data.twins)
        .await
        .map_err(|err| HttpError::FailedToSetTwin(err.to_string()))?;
    let dst: SessionID = (&envelope.destination).into();
    data.switch.send(&dst, &body).await?;

    Response::builder()
        .status(http::StatusCode::ACCEPTED)
        .body(Body::empty())
        .map_err(HttpError::Http)
}

async fn update_cache_relays(envelope: &Envelope, twin_db: &impl TwinDB) -> Result<()> {
    if envelope.relays.is_empty() {
        return Ok(());
    }
    let mut twin = twin_db
        .get_twin(envelope.source.twin.into())
        .await?
        .ok_or_else(|| anyhow::Error::msg("unknown twin source"))?;
    let envelope_relays = RelayDomains::new(&envelope.relays);
    match twin.relay {
        Some(twin_relays) => {
            if twin_relays == envelope_relays {
                return Ok(());
            }
            twin.relay = Some(envelope_relays);
        }
        None => twin.relay = Some(envelope_relays),
    }
    twin_db.set_twin(twin).await
}

type Writer = SplitSink<WebSocketStream<Upgraded>, Message>;

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

        while let Some(Some((id, data))) = self.cancellation.run_until_cancelled(rx.recv()).await {
            log::trace!("relaying message {} to peer {}", id, self.peer);
            match self
                .cancellation
                .run_until_cancelled(self.writer.send(Message::Binary(data)))
                .await
            {
                // cancelled
                None => return,
                // failed to write
                Some(Err(err)) => {
                    let _ = self.writer.close();
                    log::debug!("failed to forward message to peer: {}", err);
                    return;
                }
                // written
                Some(_) => {}
            }

            _ = self.switch.ack(&self.peer, &[id]).await;
        }

        drop(drop_guard);
    }
}

pub(crate) struct WriterCallback {
    tx: Sender<(MessageID, Vec<u8>)>,
}

impl Callback for WriterCallback {
    fn handle(&self, id: MessageID, data: Vec<u8>) -> Result<(), CallbackError> {
        self.tx.try_send((id, data)).map_err(|_| CallbackError)
    }
}

struct Session<M: Metrics, D: TwinDB> {
    id: SessionID,
    domain: HashSet<String>,
    switch: Arc<Switch<WriterCallback>>,
    federator: Arc<Federator>,
    metrics: M,
    twins: D,
}

impl<M: Metrics, D: TwinDB> Session<M, D> {
    fn new(
        claims: Claims,
        domain: HashSet<String>,
        switch: Arc<Switch<WriterCallback>>,
        federator: Arc<Federator>,
        metrics: M,
        twins: D,
    ) -> Self {
        let id = SessionID::new(claims.id.into(), claims.sid);
        Self {
            id,
            domain,
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

        // todo(sameh):
        // check if the dst twin id is already connected locally
        // if so, we don't have to check federation and directly
        // switch the message

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

        // it's safe to update the local cache since we already authenticated
        // the twin hence we trust their information.
        update_cache_relays(envelope, &self.twins).await?;

        if !twin
            .relay
            .ok_or_else(|| anyhow::Error::msg("relay info is not set for this twin"))?
            .has_common(&self.domain)
        {
            log::debug!("got an foreign message");
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
                            log::trace!("received unsupported (text) message. disconnecting!");
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
                                log::trace!("twin with stream id {} exceeded its request limits, dropping message", self.id);
                                self.send_error(envelope, "exceeded rate limits, dropping message").await;
                                continue
                            }

                            // if we failed to route back the message to the user
                            // for any reason we send an error message back
                            // server error has no source address set.
                            if let Err(err) = self.route(&mut envelope, msg).await {
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

        self.switch.unregister(self.id).await;
        drop(drop_guard);
        Ok(())
    }

    async fn send_error<E: Display>(&self, envelope: Envelope, err: E) {
        let mut resp = Envelope::new();
        resp.expiration = 300;
        resp.stamp();
        resp.uid = envelope.uid;
        resp.destination = Some((&self.id).into()).into();
        let e = resp.mut_error();
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
