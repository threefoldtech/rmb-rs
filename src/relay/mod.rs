use crate::token;
use crate::twin::TwinDB;
use crate::types::{Envelope, EnvelopeExt};
use anyhow::Result;
use hyper_tungstenite::tungstenite::error::ProtocolError;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::net::ToSocketAddrs;

mod api;
mod federation;
pub mod limiter;
mod switch;
use self::limiter::RateLimiter;
use self::ranker::RelayRanker;
use api::WriterCallback;
use federation::Federation;
pub use federation::FederationOptions;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use switch::Switch;
pub use switch::SwitchOptions;
pub mod ranker;

pub struct Relay<D: TwinDB, R: RateLimiter> {
    switch: Arc<Switch<WriterCallback>>,
    twins: D,
    domains: HashSet<String>,
    federation: Federation<D>,
    limiter: R,
}

// Build an error response envelope based on a request envelope.
// Note: destination is NOT set here; caller assigns the appropriate destination.
pub(crate) fn build_error_envelope<E: std::fmt::Display>(req_env: &Envelope, err: E) -> Envelope {
    let mut resp = Envelope::new();
    resp.expiration = 300;
    resp.stamp();
    resp.uid = req_env.uid.clone();
    let e = resp.mut_error();
    e.message = err.to_string();
    resp
}

impl<D, R> Relay<D, R>
where
    D: TwinDB + Clone,
    R: RateLimiter,
{
    pub fn new(
        domains: HashSet<String>,
        twins: D,
        opt: SwitchOptions,
        federation: FederationOptions<D>,
        limiter: R,
        ranker: RelayRanker,
    ) -> Result<Self> {
        let switch = opt.build()?;
        let federation = federation.build(switch.sink(), twins.clone(), ranker)?;
        Ok(Self {
            switch: Arc::new(switch),
            twins,
            domains,
            federation,
            limiter,
        })
    }

    pub async fn start<A: ToSocketAddrs>(self, address: A) -> Result<()> {
        let tcp_listener = TcpListener::bind(address).await?;
        let federator = self.federation.start();
        let http = api::HttpService::new(api::AppData::new(
            self.domains,
            self.switch,
            self.twins,
            federator,
            self.limiter,
        ));
        loop {
            let (tcp_stream, peer_addr) = tcp_listener.accept().await?;
            let http = http.clone();
            tokio::task::spawn(async move {
                // Reduce latency for small frames (WS pings)
                if let Err(e) = tcp_stream.set_nodelay(true) {
                    log::debug!("failed to set TCP_NODELAY: {}", e);
                }
                let io: TokioIo<tokio::net::TcpStream> = TokioIo::new(tcp_stream);
                // Auto-detect HTTP/1.1 vs HTTP/2 per-connection; allow WS upgrades on HTTP/1.1
                let mut builder = auto::Builder::new(TokioExecutor::new());
                // Restore HTTP/1.1 tuning (keep-alive + header read timeout)
                builder
                    .http1()
                    .timer(TokioTimer::new())
                    .keep_alive(true)
                    .header_read_timeout(Duration::from_secs(600));
                // Restore HTTP/2 tuning (adaptive window + keep-alive interval)
                builder
                    .http2()
                    .timer(TokioTimer::new())
                    .adaptive_window(true)
                    .keep_alive_interval(Some(Duration::from_secs(60)))
                    .keep_alive_timeout(Duration::from_secs(20));

                if let Err(http_err) = builder.serve_connection_with_upgrades(io, http).await {
                    let benign = http_err
                        .as_ref()
                        .downcast_ref::<hyper::Error>()
                        .map(|e| e.is_timeout() || e.is_incomplete_message())
                        .unwrap_or(false);
                    if benign {
                        log::debug!("conn {}: benign connection end: {}", peer_addr, http_err);
                    } else {
                        log::error!(
                            "conn {}: error while serving HTTP connection: {}",
                            peer_addr,
                            http_err
                        );
                    }
                }
            });
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum HttpError {
    #[error("missing jwt")]
    MissingJWT,
    #[error("invalid jwt: {0}")]
    InvalidJWT(#[from] token::Error),
    #[error("failed to get twin: {0}")]
    FailedToGetTwin(String),
    #[error("failed to set twin: {0}")]
    FailedToSetTwin(String),
    #[error("twin not found {0}")]
    TwinNotFound(u32),
    #[error("{0}")]
    WebsocketError(#[from] ProtocolError),
    #[error("page not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("switch error: {0}")]
    SwitchingError(#[from] switch::SwitchError),
    // generic catch all
    #[error("{0}")]
    Http(#[from] http::Error),
}

impl HttpError {
    pub fn status(&self) -> http::StatusCode {
        use http::StatusCode as Codes;
        match self {
            Self::MissingJWT => Codes::BAD_REQUEST,
            Self::InvalidJWT(_) => Codes::UNAUTHORIZED,
            Self::FailedToGetTwin(_) => Codes::INTERNAL_SERVER_ERROR,
            Self::FailedToSetTwin(_) => Codes::INTERNAL_SERVER_ERROR,
            Self::TwinNotFound(_) => Codes::UNAUTHORIZED,
            Self::WebsocketError(_) => Codes::INTERNAL_SERVER_ERROR,
            Self::NotFound => Codes::NOT_FOUND,
            Self::BadRequest(_) => Codes::BAD_REQUEST,
            Self::SwitchingError(_) => Codes::INTERNAL_SERVER_ERROR,
            Self::Http(_) => Codes::INTERNAL_SERVER_ERROR,
        }
    }
}
