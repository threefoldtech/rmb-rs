use std::path::PathBuf;

use super::Bag;
use super::Plugin;
use crate::peer::postman::Generator;
use crate::peer::storage::{JsonError, Storage};
use crate::peer::storage::{JsonIncomingResponse, JsonOutgoingRequest};
use crate::types::Address;
use crate::types::Backlog;
use crate::types::Envelope;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc::Sender, Mutex};

const MODULE: &str = "file";
const FILE_UPLOAD: &str = "file.upload";
const FILE_NEGOTIATE: &str = "file.negotiate";
const FILE_WRITE: &str = "file.write";

const SCHEME: &str = "application/json";

#[derive(Deserialize)]
struct UploadRequestBody {
    // path of the file being uploaded
    path: String,
    // cmd on remote end to call once files is ready
    cmd: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct UploadOpen {
    size: u64,
}

// in case we need to extend the upload process
// with more stages, we can add more steps here
// but so far we only have the remote open
#[derive(Default)]
enum UploadState {
    // sent request to remote peer, to start an upload
    // the request can either timeout, succeed (if upload is allowed)
    // or fail
    #[default]
    RemoteOpen,
}

enum LocalResp {
    Error(String),
    Data(String),
    Empty,
}

// an upload operation
struct UploadJob {
    // the original request
    request: UploadRequestBody,
    // the size of the upload
    size: u64,

    dst: u32,
    // the state of the operation
    state: UploadState,
}

pub struct Upload<S>
where
    S: Storage,
{
    ch: Option<Sender<Bag>>,
    uploads: Arc<Mutex<HashMap<String, UploadJob>>>,
    storage: S,
    dir: PathBuf,
}

impl<S> Upload<S>
where
    S: Storage,
{
    pub fn new<P: Into<PathBuf>>(storage: S, dir: P) -> Self {
        Self {
            storage,
            uploads: Arc::new(Mutex::new(HashMap::default())),
            ch: None,
            dir: dir.into(),
        }
    }

    async fn sender_upload(&self, request: &JsonOutgoingRequest) -> Result<()> {
        // initial checks of the file.
        if request.destinations.len() != 1 {
            bail!("can only upload to one destination");
        }

        let body: UploadRequestBody = serde_json::from_slice(
            &base64::decode(&request.data).context("invalid body encoding")?,
        )
        .context("invalid json request")?;

        let meta = fs::metadata(&body.path)
            .await
            .context("failed to get file state")?;

        if !meta.is_file() {
            bail!("path '{}' is not a file", body.path);
        }

        let operation = UploadJob {
            request: body,
            size: meta.len(),
            dst: request.destinations[0],
            state: UploadState::RemoteOpen,
        };
        let upload_id = uuid::Uuid::new_v4().to_string();
        let mut uploads = self.uploads.lock().await;
        uploads.insert(upload_id.clone(), operation);

        //TODO: clean up envelope creation maybe
        let mut env = Envelope {
            uid: upload_id.clone(),
            destination: address(request.destinations[0]).into(),
            ..Default::default()
        };

        let msg = env.mut_request();
        msg.command = FILE_NEGOTIATE.into();
        let data = serde_json::to_vec(&UploadOpen { size: meta.len() })
            .context("failed to build request")?;

        env.set_plain(data);

        // we send this message to remote peer while we have a "state" of the upload
        // if we ever received a response that upload is possible, we can then

        //the backlog is needed to route any response for that message back to this module
        let bag = Bag::one(env).backlog(Backlog {
            uid: upload_id,
            module: MODULE.into(), //module is needed to route responses back here.
            reply_to: request.reply_to.clone(),
            reference: request.reference.clone(),
            ..Default::default()
        });

        if self.ch.as_ref().unwrap().send(bag).await.is_err() {
            bail!("failed to schedule message for sending");
        }

        Ok(())
    }

    async fn send_local(&self, queue: &str, reference: &Option<String>, resp: LocalResp) {
        let (data, err) = match resp {
            LocalResp::Data(data) => (data, None),
            LocalResp::Error(msg) => (String::default(), Some(msg)),
            LocalResp::Empty => (String::default(), None),
        };

        _ = self
            .storage
            .response(
                queue,
                JsonIncomingResponse {
                    version: 1,
                    reference: reference.clone(),
                    data: base64::encode(data),
                    source: String::default(),
                    schema: Some(SCHEME.into()),
                    timestamp: 0,
                    error: err.map(|e| JsonError {
                        code: 0,
                        message: e,
                    }),
                },
            )
            .await;
    }

    async fn request_negotiate(&self, request: &Envelope) -> Result<()> {
        // TODO: add check if size is acceptable
        let mut env = Envelope {
            uid: request.uid.clone(),
            destination: request.source.clone(),
            ..Default::default()
        };
        // mark as response
        env.mut_response();
        // send back
        _ = self.ch.as_ref().unwrap().send(Bag::one(env)).await;

        Ok(())
    }

    /// handles a request from a remote peer
    async fn remote_request(&self, request: &Envelope) -> Result<()> {
        if !request.has_request() {
            // should not happen
            bail!("invalid request message");
        }
        let req = request.request();
        match req.command.as_str() {
            FILE_NEGOTIATE => {
                // a remote (sender) peer is negotiating a file upload!
                // if we already here then we know that file
                // upload is already enabled, and we then can
                // just accept or reject this request
                self.request_negotiate(request).await
            }
            FILE_WRITE => {
                // we got a write request
                log::info!("writing to file: {}", request.uid);
                Ok(())
            }
            _ => Err(anyhow::anyhow!("unknown command: {}", req.command)),
        }
    }

    /// handles responses from a remote peer
    async fn remote_response(&self, tracker: Backlog, response: &Envelope) {
        let uploads = self.uploads.lock().await;
        let upload = match uploads.get(&tracker.uid) {
            None => {
                log::error!("received response for an upload that does not exist");
                return;
            }
            Some(upload) => upload,
        };

        if response.has_error() {
            let err = response.error();

            return self
                .send_local(
                    &tracker.reply_to,
                    &tracker.reference,
                    LocalResp::Error(err.message.clone()),
                )
                .await;
        }

        // okay now we have a response to an upload, we can then advance the state
        // and move to the next stage.
        match upload.state {
            UploadState::RemoteOpen => {
                // this is the only state right now.
                log::info!("starting a file upload: {}", upload.request.path);
                // send success operation to client tell him
                // that upload will start

                if let Err(err) = self.start_upload(&tracker, upload).await {
                    // if we failed to start the upload, send back an error
                    self.send_local(
                        &tracker.reply_to,
                        &tracker.reference,
                        LocalResp::Error(err.to_string()),
                    )
                    .await;

                    return;
                }
                // otherwise send back the upload id.
                // send the upload id to the local caller
                let data = serde_json::to_string(&tracker.uid).unwrap();
                // send local response
                self.send_local(&tracker.reply_to, &tracker.reference, LocalResp::Data(data))
                    .await;
            }
        }
    }

    async fn start_upload(&self, tracker: &Backlog, job: &UploadJob) -> Result<()> {
        let file = fs::File::open(&job.request.path)
            .await
            .context("failed to open file for reading")?;

        let mut base = Envelope {
            uid: tracker.uid.clone(),
            destination: address(job.dst).into(),
            ..Default::default()
        };

        let req = base.mut_request();
        req.command = "file.write".into();

        // TODO: the tracker can timeout, so we need to set a bigger ttl, or find another mechanism to
        // keep tracking of uploads ids.
        //
        // we also need to tell the other peer that the upload is complete. this can be done by sending
        // a last message with the file.commit message may be
        let bag = Bag::new(ChunkGenerator::new(base, file, 512 * 1024)).backlog(tracker.clone());

        self.ch
            .as_ref()
            .unwrap()
            .send(bag)
            .await
            .map_err(|_| anyhow::anyhow!("failed to push message for sending"))
    }
}

#[async_trait::async_trait]
impl<S> Plugin for Upload<S>
where
    S: Storage,
{
    fn name(&self) -> &str {
        MODULE
    }

    /// handles local requests (that are initiated from a local client)
    async fn local(&self, request: JsonOutgoingRequest) -> Option<JsonOutgoingRequest> {
        // intercepts the user request to upload a file
        // so we assume that this can ONLY be `file.upload` with proper request body
        // that goes as {path, cmd}

        // check schema
        if !matches!(request.schema, Some(ref schema) if schema == "application/json") {
            self.send_local(
                &request.reply_to,
                &request.reference,
                LocalResp::Error("expected content schema to be application/json".into()),
            )
            .await;
            return None;
        }

        let result = match request.command.as_str() {
            FILE_UPLOAD => self.sender_upload(&request).await,
            _ => Err(anyhow::anyhow!("unknown command: {}", request.command)),
        };

        if let Err(err) = result {
            self.send_local(
                &request.reply_to,
                &request.reference,
                LocalResp::Error(err.to_string()),
            )
            .await;
        }

        // hijack. return none here so that the request is not
        // sent to the peer since we gonna send our own custom messages
        None
    }

    async fn remote(&self, tracker: Option<Backlog>, incoming: &Envelope) {
        if let Some(tracker) = tracker {
            return self.remote_response(tracker, incoming).await;
        }

        // otherwise is a request
        if let Err(err) = self.remote_request(incoming).await {
            // not we are here on a remote peer, then sending an error
            // back requires sending an envelope
            let mut env = Envelope {
                uid: incoming.uid.clone(),
                destination: incoming.source.clone(),
                ..Default::default()
            };

            let e = env.mut_error();
            e.message = format!("{:#}", err);

            if self.ch.as_ref().unwrap().send(Bag::one(env)).await.is_err() {
                log::error!("failed to send error message back to caller");
            }
        }
    }

    fn start(&mut self, sender: Sender<Bag>) {
        self.ch = Some(sender)
    }
}

fn address(a: u32) -> Option<Address> {
    Some(Address {
        twin: a,
        ..Default::default()
    })
}

/// a file iter generates envelops that carries file chunks
struct ChunkGenerator {
    base: Envelope,
    inner: BufReader<fs::File>,
    buffer: Vec<u8>,
}

impl ChunkGenerator {
    fn new(base: Envelope, f: fs::File, bs: usize) -> Self {
        Self {
            base,
            inner: BufReader::new(f),
            buffer: vec![0; bs],
        }
    }

    async fn next_env(&mut self) -> Result<Option<Envelope>> {
        let len = self
            .inner
            .read(&mut self.buffer)
            .await
            .context("failed to read file")?;

        log::debug!("read {} bytes", len);
        if len == 0 {
            return Ok(None);
        }

        let mut env = self.base.clone();
        env.set_plain(self.buffer[..len].into());
        log::debug!("sending chunk");
        Ok(Some(env))
    }
}
#[async_trait::async_trait]
impl Generator for ChunkGenerator {
    async fn next(&mut self) -> Option<Envelope> {
        match self.next_env().await {
            Ok(r) => r,
            Err(err) => {
                log::error!("error while reading file data: {:#}", err);
                None
            }
        }
    }
}
