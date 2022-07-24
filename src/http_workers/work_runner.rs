use async_trait::async_trait;
use hyper::{client::HttpConnector, header, Body, Client, Method, Request};
use mpart_async::client::MultipartRequest;

use crate::{
    identity::Signer,
    storage::Storage,
    twin::{Twin, TwinDB},
    types::{Message, TransitMessage, UploadPayload},
};

use workers::Work;

use anyhow::{Context, Result};

#[derive(Eq, PartialEq)]
enum Queue {
    Request,
    Reply,
    Upload,
}

impl std::convert::AsRef<str> for Queue {
    fn as_ref(&self) -> &str {
        match self {
            Self::Request => "zbus-remote",
            Self::Reply => "zbus-reply",
            Self::Upload => "zbus-upload",
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum SendError {
    #[error("receive error reply from remote rmb: `{0}`")]
    Terminal(String),

    #[error("failed to deliver message to remote rmb: {0:#}")]
    Error(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct WorkRunner<T, I, S>
where
    T: TwinDB,
    I: Signer,
    S: Storage,
{
    twin_db: T,
    identity: I,
    storage: S,
    client: Client<HttpConnector, Body>,
}

impl<T, I, S> WorkRunner<T, I, S>
where
    T: TwinDB,
    I: Signer,
    S: Storage,
{
    pub fn new(twin_db: T, identity: I, storage: S, client: Client<HttpConnector, Body>) -> Self {
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

    async fn upload_once<U: AsRef<str>>(
        &self,
        twin: &Twin,
        uri_path: U,
        msg: &mut Message,
        payload: UploadPayload,
    ) -> Result<(), SendError> {
        let mut mpart = MultipartRequest::default();

        mpart.add_file("upload", payload.path);
        let uri = uri_builder(uri_path, &twin.address);
        log::debug!("sending upload to '{}'", uri);

        let request = Request::post(uri)
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={}", mpart.get_boundary()),
            )
            .header("rmb-upload-cmd", payload.cmd)
            .header("rmb-source-id", payload.source)
            .header("rmb-timestamp", payload.timestamp)
            .header(
                "rmb-signature",
                payload.signature.unwrap_or_else(|| "".to_string()),
            )
            .body(Body::wrap_stream(mpart))
            .map_err(|err| anyhow!("{:?}", err))?;

        let response = self
            .client
            .request(request)
            .await
            .map_err(|err| anyhow!("{:?}", err))?;

        let status = response.status();
        if status != http::StatusCode::ACCEPTED {
            return Err(SendError::Terminal(format!(
                "upload failed with status code of {}",
                status
            )));
        }

        self.handle_upload_success(twin.id, msg).await;

        Ok(())
    }

    async fn send_once<U: AsRef<str>>(
        &self,
        twin: &Twin,
        uri_path: U,
        msg: &mut Message,
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

    async fn send(
        &self,
        id: u32,
        retry: usize,
        queue: &Queue,
        msg: &mut Message,
    ) -> Result<(), SendError> {
        // getting twin object
        let twin = match self.get_twin(id, retry).await {
            Some(twin) => twin,
            None => {
                return Err(SendError::Terminal(format!(
                    "twin with id {} not found",
                    id
                )))
            }
        };

        // we always reset the tag to none before
        // sending to remote
        msg.tag = None;

        // posting the message to the remote twin
        let mut result = Ok(());
        for _ in 0..retry {
            msg.stamp();
            msg.valid()
                .context("message validation failed")
                .map_err(SendError::Error)?;

            // signing the message
            msg.sign(&self.identity);

            match queue {
                Queue::Upload => {
                    // verify if it's uploadable and get the payload stamped
                    // or fail as early as possible, then sign
                    let mut upload_payload: UploadPayload = msg.try_into()?;
                    upload_payload.sign(&self.identity);
                    result = self.upload_once(&twin, &queue, msg, upload_payload).await;
                }
                Queue::Request => result = self.send_once(&twin, &queue, msg).await,
                Queue::Reply => (),
            }

            if let Err(SendError::Error(_)) = &result {
                continue;
            }

            break;
        }

        result
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
            log::error!("failed to deliver failure response: {}", err);
        }
    }

    async fn handle_upload_success(&self, twin: u32, msg: &mut Message) {
        let dst = vec![msg.source];
        msg.source = twin;
        msg.destination = dst;
        msg.error = None;
        msg.data = base64::encode("success");

        if let Err(err) = self
            .storage
            .reply(msg)
            .await
            .context("can not send a reply message")
        {
            log::error!("failed to deliver upload success response: {}", err);
        }
    }
}

#[async_trait]
impl<T, I, S> Work for WorkRunner<T, I, S>
where
    T: TwinDB,
    I: Signer,
    S: Storage,
{
    type Input = TransitMessage;
    type Output = ();

    async fn run(&self, job: Self::Input) {
        //identify uri and extract msg
        log::debug!("http worker received a job");
        let (queue, mut msg) = match job {
            TransitMessage::Request(msg) => (Queue::Request, msg),
            TransitMessage::Reply(msg) => (Queue::Reply, msg),
            TransitMessage::Upload(msg) => (Queue::Upload, msg),
        };

        log::debug!("received a message for forwarding '{}'", queue.as_ref());
        let retry = Self::retries(msg.retry);
        assert_eq!(
            msg.destination.len(),
            1,
            "expecting only one destination in worker"
        );
        let id = msg.destination[0];

        log::debug!(
            "forwarding message to destination '{}' '{}'",
            id,
            queue.as_ref()
        );

        if let Err(err) = self.send(id, retry, &queue, &mut msg).await {
            if queue == Queue::Request || queue == Queue::Upload {
                self.handle_delivery_err(id, msg, err).await;
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

        let u = uri_builder("rmb-remote", "example.com");
        assert_eq!(u, "http://example.com:8051/rmb-remote");

        let u = uri_builder("rmb-remote", "example.com:1234");
        assert_eq!(u, "http://example.com:1234/rmb-remote");
    }
}
