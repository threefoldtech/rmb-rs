use crate::cache::RedisCache;
use crate::token;
use crate::twin::TwinDB;
use anyhow::Result;
use hyper::server::conn::Http;
use hyper_tungstenite::tungstenite::error::ProtocolError;
use tokio::net::TcpListener;
use tokio::net::ToSocketAddrs;

mod api;
mod switch;
pub mod rate_limiter;

use api::RelayHook;
use switch::Switch;
pub use switch::SwitchOptions;
use rate_limiter::{Throttler, ThrottlerCache};
pub struct Relay<D: TwinDB, T: ThrottlerCache> {
    switch: Switch<RelayHook>,
    twins: D,
    domain: String,
    throttler: Throttler<T>
}

impl<D, T> Relay<D, T>
where
    D: TwinDB + Clone,
    T: ThrottlerCache + Clone,
{
    pub async fn new<S: Into<String>>(domain: S, twins: D, opt: SwitchOptions, throttler: Throttler<T>) -> Result<Self> {
        let switch = opt.build().await?;
        Ok(Self {
            switch,
            twins,
            domain: domain.into(),
            throttler,
        })
    }

    pub async fn start<A: ToSocketAddrs>(self, address: A) -> Result<()> {
        let tcp_listener = TcpListener::bind(address).await?;

        let http = api::HttpService::new(api::AppData::new(self.domain, self.switch, self.twins, self.throttler));
        
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
            Self::TwinNotFound(_) => Codes::UNAUTHORIZED,
            Self::WebsocketError(_) => Codes::INTERNAL_SERVER_ERROR,
            Self::NotFound => Codes::NOT_FOUND,
            Self::BadRequest(_) => Codes::BAD_REQUEST,
            Self::SwitchingError(_) => Codes::INTERNAL_SERVER_ERROR,
            Self::Http(_) => Codes::INTERNAL_SERVER_ERROR,
        }
    }
}
