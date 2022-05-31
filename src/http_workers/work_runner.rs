use async_trait::async_trait;
use hyper::{client::HttpConnector, Body, Client, Method, Request};

use crate::{
    cache::Cache,
    identity::Signer,
    storage::Storage,
    twin::{SubstrateTwinDB, Twin, TwinDB},
    types::{Message, QueuedMessage},
    workers::Work,
};

use anyhow::{Context, Result};

#[derive(PartialEq)]
enum Queue {
    Forward,
    Reply,
}

impl std::convert::AsRef<str> for Queue {
    fn as_ref(&self) -> &str {
        match self {
            Self::Forward => "zbus-remote",
            Self::Reply => "zbus-reply",
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum SendError {
    #[error("receive error reply from remote rmb: `{0}`")]
    Terminal(String),

    #[error("failed to deliver message to remote rmb: {0}")]
    Error(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Signer,
    S: Storage,
{
    twin_db: SubstrateTwinDB<C>,
    identity: I,
    storage: S,
    client: Client<HttpConnector, Body>,
}

impl<C, I, S> WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Signer,
    S: Storage,
{
    pub fn new(
        twin_db: SubstrateTwinDB<C>,
        identity: I,
        storage: S,
        client: Client<HttpConnector, Body>,
    ) -> Self {
        Self {
            twin_db,
            identity,
            storage,
            client,
        }
    }

    async fn get_twin(&self, twin_id: u32, retires: usize) -> Option<Twin> {
        for _ in 0..retires {
            let twin = self.twin_db.get_twin(twin_id.to_owned() as u32).await;

            match twin {
                Ok(twin) => {
                    return twin;
                }
                Err(err) => {
                    log::debug!(
                        "can not retrieve twin from substrate for this id '{}' because of {}",
                        twin_id,
                        err
                    );
                }
            };
        }
        None
    }

    fn retries(u: usize) -> usize {
        if u == 0 {
            1
        } else if u > 5 {
            5
        } else {
            u
        }
    }

    fn encrypt_dat(dat: String, _twin: &Twin) -> Result<String> {
        Ok(dat)
    }

    async fn send_msg<U: AsRef<str>>(
        &self,
        twin: &Twin,
        uri_path: U,
        msg: &Message,
    ) -> Result<(), SendError> {
        log::debug!("sending message");
        let req = Request::builder()
            .method(Method::POST)
            .header("content-type", "application/json");

        let uri = uri_builder(uri_path, &twin.address);
        log::debug!("sending message to '{}'", uri);
        let req = req.uri(uri);

        let req = req
            .body(Body::from(serde_json::to_vec(&msg).unwrap()))
            .with_context(|| format!("can not construct request body for this id '{}'", twin.id))?;

        let response = self
            .client
            .request(req)
            .await
            .context("failure of message delivery!")?;

        // log::debug!("received response: {:?}", response.body());
        // at this point, the remote rmb received our request but then replied with
        // wrong status code (it didn't accept the message for some reason)
        // hence we assume this is a terminal error, and retrying won't fix it.
        // We match with Accepted or Ok too for backward compatibility
        if response.status() != http::StatusCode::ACCEPTED
            && response.status() != http::StatusCode::OK
        {
            let status = response.status();
            if let Ok(bytes) = hyper::body::to_bytes(response.into_body()).await {
                log::error!("response: {:?}", String::from_utf8(bytes.to_vec()));
            };

            Err(SendError::Terminal(format!(
                "received error {} from remote twin",
                status
            )))
        } else {
            Ok(())
        }
    }

    async fn handle_delivery_err(&self, twin: u32, mut msg: Message, err: SendError) {
        let dst = vec![msg.source];
        msg.source = twin;
        msg.destination = dst;
        msg.error = Some(format!("{}", err));
        msg.data = String::default();

        if let Err(err) = self
            .storage
            .reply(&msg)
            .await
            .context("can not send a reply message")
        {
            log::error!("{:?}", err);
        }
    }
}

#[async_trait]
impl<C, I, S> Work for WorkRunner<C, I, S>
where
    C: Cache<Twin>,
    I: Signer,
    S: Storage,
{
    type Job = QueuedMessage;

    async fn run(&self, job: Self::Job) {
        //identify uri and extract msg
        log::debug!("http worker received a job");
        let (queue, msg) = match job {
            QueuedMessage::Forward(msg) => (Queue::Forward, msg),
            QueuedMessage::Reply(msg) => (Queue::Reply, msg),
        };

        log::debug!("received a message for forwarding '{}'", queue.as_ref());
        let retry = Self::retries(msg.retry);
        for id in &msg.destination {
            log::debug!(
                "forwarding message to destination '{}' '{}'",
                id,
                queue.as_ref()
            );
            let mut msg = msg.clone();
            //let uri_path = uri_path.clone();
            // getting twin object
            let twin = match self.get_twin(*id, retry).await {
                Some(twin) => twin,
                None => {
                    continue;
                }
            };

            // encrypt dat
            msg.data = match Self::encrypt_dat(msg.data, &twin) {
                Ok(dat) => dat,
                Err(_err) => {
                    todo!()
                }
            };

            // set time
            msg.set_now();

            // signing the message
            msg.sign(&self.identity);

            // posting the message to the remote twin
            let mut result = Ok(());
            for _ in 0..retry {
                result = self.send_msg(&twin, &queue, &msg).await;
                if let Err(SendError::Error(_)) = &result {
                    continue;
                }
                break;
            }

            if result.is_err() && queue == Queue::Forward {
                self.handle_delivery_err(twin.id, msg, result.err().unwrap())
                    .await;
            }
        }
    }
}

fn uri_builder<U: AsRef<str>, A: AsRef<str>>(uri_path: U, twin_address: A) -> String {
    // twin_address can be ipv4 or ipv6. it also can have port or not. so acceptable formats
    // can be
    // - ipv4 (192.168.1.1)
    // - ipv6 (305:c436:d34d:7be:21bc:ce3d:7927:1zbc)
    // - ip:port
    // - address.com
    // - address.com:port
    const DEFAULT_PORT: u16 = 8051;
    use std::net::{IpAddr, SocketAddr};
    let twin_address = twin_address.as_ref();

    let mkhost = |ip: IpAddr, port: u16| match ip {
        IpAddr::V4(addr) => format!("{}:{}", addr, port),
        IpAddr::V6(addr) => format!("[{}]:{}", addr, port),
    };

    // Authority::from_parts(None, None, , port)
    let host = match twin_address.parse::<IpAddr>() {
        Ok(addr) => mkhost(addr, DEFAULT_PORT),
        Err(_) => match twin_address.parse::<SocketAddr>() {
            Ok(addr) => mkhost(addr.ip(), addr.port()),
            Err(_) => {
                // we assume it's a name. then it can be either like
                // example.com
                // or
                // example.com:1824
                if twin_address.rfind(':').is_none() {
                    // no port data
                    format!("{}:{}", twin_address, DEFAULT_PORT)
                } else {
                    twin_address.into()
                }
            }
        },
    };

    format!("http://{}/{}", host, uri_path.as_ref())
}

#[cfg(test)]
mod test {
    #[test]
    fn uri() {
        use super::uri_builder;
        let u = uri_builder("rmb-remote", "305:c436:d34d:7be:21bc:ce3d:7927:1ebc");
        assert_eq!(
            u,
            "http://[305:c436:d34d:7be:21bc:ce3d:7927:1ebc]:8051/rmb-remote"
        );

        let u = uri_builder("rmb-remote", "[305:c436:d34d:7be:21bc:ce3d:7927:1ebc]:1234");
        assert_eq!(
            u,
            "http://[305:c436:d34d:7be:21bc:ce3d:7927:1ebc]:1234/rmb-remote"
        );

        let u = uri_builder("rmb-remote", "192.168.12.34");
        assert_eq!(u, "http://192.168.12.34:8051/rmb-remote");

        let u = uri_builder("rmb-remote", "192.168.12.34:5678");
        assert_eq!(u, "http://192.168.12.34:5678/rmb-remote");
    }
}
