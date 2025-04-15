use crate::token;
use crate::twin::TwinDB;
use anyhow::Result;
use hyper::server::conn::Http;
use hyper_tungstenite::tungstenite::error::ProtocolError;
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

impl<D, R> Relay<D, R>
where
    D: TwinDB + Clone,
    R: RateLimiter,
{
    pub async fn new(
        domains: HashSet<String>,
        twins: D,
        opt: SwitchOptions,
        federation: FederationOptions<D>,
        limiter: R,
        ranker: RelayRanker,
    ) -> Result<Self> {
        let switch = opt.build().await?;
        let federation = federation.build(switch.sink(), twins.clone(), ranker)?;
        Ok(Self {
            switch: Arc::new(switch),
            twins,
            domains: domains,
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
