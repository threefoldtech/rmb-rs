use crate::token;
use crate::twin::TwinDB;
use anyhow::Result;
use hyper::server::conn::Http;
use hyper_tungstenite::tungstenite::error::ProtocolError;
use tokio::net::TcpListener;
use tokio::net::ToSocketAddrs;

mod api;
mod federation;
mod switch;
use api::RelayHook;
pub use federation::Federation;
use switch::Switch;
pub use switch::SwitchOptions;

pub struct Relay<D: TwinDB> {
    switch: Switch<RelayHook>,
    twins: D,
    domain: String,
    federation: Federation,
}

impl<D> Relay<D>
where
    D: TwinDB + Clone,
{
    pub async fn new<S: Into<String>>(
        domain: S,
        twins: D,
        opt: SwitchOptions,
        federation: Federation,
    ) -> Result<Self> {
        let switch = opt.build().await?;
        Ok(Self {
            switch,
            twins,
            domain: domain.into(),
            federation,
        })
    }

    pub async fn start<A: ToSocketAddrs>(self, address: A) -> Result<()> {
        let tcp_listener = TcpListener::bind(address).await?;
        let federation_workers = self.federation.clone();
        tokio::task::spawn(federation_workers.start());
        let http = api::HttpService::new(api::AppData::new(
            self.domain,
            self.switch,
            self.twins,
            self.federation,
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use crate::{
        redis,
        types::{Envelope, EnvelopeExt}, twin::{SubstrateTwinDB, self}, cache::{RedisCache, self}, identity, peer,
    };
    use bb8_redis::redis::aio::ConnectionLike;
    use httpmock::{MockServer, Method::POST};
    use switch::SwitchOptions;
    pub(crate) use identity::KeyType;
    use identity::{Identity, Signer};
    pub use peer::{ storage::RedisStorage};
    use twin::{ TwinDB};
    use bb8_redis::{bb8::Pool, redis::cmd, RedisConnectionManager};
    use protobuf::Message;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_federation() {
        tokio::spawn(async {
            let pool_size = 10;
            let pool = redis::pool("redis://localhost:6379", pool_size)
                .await
                .unwrap();

            // we use 6 hours cache for twin information because twin id will not change anyway
            // and we only need twin public key for validation only.
            let twins = SubstrateTwinDB::<RedisCache>::new(
                "wss://tfchain.grid.tf:443",
                RedisCache::new(pool.clone(), "twin", Duration::from_secs(21600)),
            )
            .await
            .unwrap();

            let opt = SwitchOptions::new(pool.clone())
                .with_workers(10)
                .with_max_users(10 as usize * 100 as usize);

            let federation = Federation::new(pool).with_workers(10 as usize);
            let r = Relay::new("http:://localhost", twins, opt, federation)
                .await
                .unwrap();
            println!("starting relay server...");
            r.start("[::]:8080").await.unwrap();
        });

        tokio::spawn(async {
            let secret = "route visual hundred rabbit wet crunch ice castle milk model inherit outside";
            let identity= identity::Sr25519Signer::try_from(secret).unwrap();
            let pool = redis::pool("redis://localhost:6379", 10).await.unwrap();
        
            let db = SubstrateTwinDB::<RedisCache>::new(
                "wss://tfchain.dev.grid.tf:443",
                RedisCache::new(pool.clone(), "twin", Duration::from_secs(600)),
            )
            .await
            .unwrap();
        
            let id = db
                .get_twin_with_account(identity.account())
                .await
                .unwrap().unwrap();
        
            let storage = RedisStorage::builder(pool).build();
            log::info!("twin: {}", id);
        
            let u = url::Url::parse("http://127.0.0.1:8080").unwrap();
            std::thread::sleep(std::time::Duration::from_secs(1));
            println!("starting rmb peer...");
            peer::start(u, id, identity, storage, db).await.unwrap();
        });

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                .header("content-type", "text/html")
                .body("ohi");
        });

        let pool = redis::pool("redis://localhost:6379", 10).await.unwrap();
        let mut con = pool.get().await.unwrap();
        // let msg: Vec<u8> = Vec::new();
        println!("executing lpush....");
        let msg = peer::storage::JsonOutgoingRequest{
            command: String::default(),
            data: String::default(),
            reply_to: String::default(),
            destinations: Vec::new(),
            expiration: 100000,
            timestamp: std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs(),
            reference: Some(String::default()),
            schema: Some(String::default()),
            tags: Some(String::default()),
            version: 0,
        };
        
        
        let x = cmd("LPUSH")
            .arg("msgbus.system.local")
            .arg(msg)
            .query_async::<_, u32>(&mut *con).await.unwrap();
        println!("x has retured from redis: {}", x);
        

        std::thread::sleep(std::time::Duration::from_secs(20));

        mock.assert_hits(1);
        
    }
}
