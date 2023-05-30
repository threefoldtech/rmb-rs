use std::fmt::Display;
use std::path::PathBuf;

use super::Bag;
use super::Plugin;
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
use tokio::sync::{mpsc::Sender, Mutex};

const MODULE: &str = "file";
const FILE_UPLOAD: &str = "file.upload";
const FILE_NEGOTIATE: &str = "file.negotiate";

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

// an upload operation
struct Operation {
    // the original request
    request: UploadRequestBody,
    // the size of the upload
    size: u64,
    // the state of the operation
    state: UploadState,
}

pub struct Upload<S>
where
    S: Storage,
{
    ch: Option<Sender<Bag>>,
    uploads: Arc<Mutex<HashMap<String, Operation>>>,
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

        let operation = Operation {
            request: body,
            size: meta.len(),
            state: UploadState::RemoteOpen,
        };
        let upload_id = uuid::Uuid::new_v4().to_string();
        let mut uploads = self.uploads.lock().await;
        uploads.insert(upload_id.clone(), operation);

        //TODO: clean up envelope creation maybe
        let mut env = Envelope {
            uid: upload_id.clone(),
            destination: address(request.destinations[0]).into(),
            expiration: 300,
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

    async fn local_err<M: Display>(&self, queue: &str, reference: &Option<String>, msg: M) {
        _ = self
            .storage
            .response(
                queue,
                JsonIncomingResponse {
                    version: 1,
                    reference: reference.clone(),
                    data: String::default(),
                    source: String::default(),
                    schema: None,
                    timestamp: 0,
                    error: Some(JsonError {
                        code: 0,
                        // we using format and not `to_string()` to also
                        // print context
                        message: format!("{:#}", msg),
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
            expiration: 300,
            ..Default::default()
        };
        // mark as response
        env.mut_response();
        // send back
        _ = self.ch.as_ref().unwrap().send(Bag::one(env)).await;

        Ok(())
    }

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
            _ => Err(anyhow::anyhow!("unknown command: {}", req.command)),
        }
    }

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
                .local_err(&tracker.reply_to, &tracker.reference, &err.message)
                .await;
        }

        // okay now we have a response to an upload, we can then advance the state
        // and move to the next stage.
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

    async fn local(&self, request: JsonOutgoingRequest) -> Option<JsonOutgoingRequest> {
        // intercepts the user request to upload a file
        // so we assume that this can ONLY be `file.upload` with proper request body
        // that goes as {path, cmd}

        // check schema
        if !matches!(request.schema, Some(ref schema) if schema == "application/json") {
            self.local_err(
                &request.reply_to,
                &request.reference,
                "expected content schema to be application/json",
            )
            .await;
            return None;
        }

        let result = match request.command.as_str() {
            FILE_UPLOAD => self.sender_upload(&request).await,
            _ => Err(anyhow::anyhow!("unknown command: {}", request.command)),
        };

        if let Err(err) = result {
            self.local_err(&request.reply_to, &request.reference, err)
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
                expiration: 300,
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
