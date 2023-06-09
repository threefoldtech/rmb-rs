//! Implementation of the upload plugin. The file upload while is a very simple concept
//! it gets more complex in a async peer to peer setup. Here is a list of the challenges
//! that needed to be taken into account while building this:
//!
//! - Peers can only communicate asynchronously. In other words, when a request is send
//!   there is no grantee a response will be sent back. All what u can do is wait on the receiver
//!   channel to see if the expected response is received
//! - A remote peer can be offline, while (of course) the relay is online. Relay keep messages in a buffer for some time
//!   until the peer is online again. means `sending` a message never fails even if the peer is offline hence a local peer
//!   can only assume peer is offline if a response is not received in a proper window of time.
//! - A remote peer doesn't have to support or allow file uploads, hence a negotiation of the upload has to be done a head of
//!   the actual upload
//! - A remote peer can die during an upload, which will cause the uploaded parts to queue on the relay side but the local peer
//!   will not know about it. To work around this an ack can be waited between each upload chunk but this can slow the entire system
//!   down [not implemented]. Right now the entire file is sent over 512k messages straight away after negotiation
//!
//! the system (ideally) works as following:
//!
//! - A client request its local peer to upload a file, given the local file `path` and a remote `cmd` to call
//!   after the upload is complete.
//! - The local peer receive this request, and it starts to do some checks including the validity of the file path, etc...
//!   We can also set a limit on the file size at this stage. An error is sent to the local client if validation fails
//! - Otherwise a negotiation request is sent to the remote peer `file.negotiate` with the requested upload size.
//! - The remote peer can either return success or error, or not send a response at all (not handled yet)
//! - In case of a success, a local response is sent to the local client with the job id
//!   - The upload is started as explained below
//! - In case of an error, a local response is sent to the local client with the cause of th error
//! - [need to handle timeout]
//!
//! In case of file negotiation success the local peer will just send a stream of messages with cmd file.write each message carries
//! a chunk of data of 512K bytes.
//!
//! So far there is no negotiation of th rate of sending since this is totally up to the relay to queue. A problem can occur (that is not handled)
//! if the relay is started with a rate limiter which allows it to drop messages if they are coming to fast or too big. this is also not handled
//! yet in this plugin.
//!
//! The upload finish with a `file.close` call that should terminate the upload job and signal the remote peer that there are no more chunks
//! to be received
//!
//! ### TODO:
//! Here is a list of the things that need to be done
//! - [ ] [important] Handle timeout in both sender and receiver sides
//! - [ ] Support query the current upload state, this is possible with another local command to query the state
//! - [ ] Remote peer once file is received need to call the local cmd with the local file path
use std::path::Path;

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
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::{mpsc::Sender, Mutex};

const MODULE: &str = "file";
const FILE_UPLOAD: &str = "file.upload";
const FILE_NEGOTIATE: &str = "file.negotiate";
const FILE_WRITE: &str = "file.write";
const FILE_CLOSE: &str = "file.close";

const FILE_CHUNK_SIZE: usize = 512 * 1024;
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
    cmd: Option<String>,
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
    Uploading,
}

enum LocalResp {
    Error(String),
    Data(String),
    #[allow(dead_code)]
    Empty,
}

// an upload operation
struct UploadJob {
    // the original request
    request: UploadRequestBody,
    // the size of the upload
    #[allow(dead_code)]
    size: u64,

    dst: u32,
    // the state of the operation
    state: UploadState,
}

struct DownloadJob {
    output: fs::File,
}

pub struct Upload<S>
where
    S: Storage,
{
    // sender channel
    ch: Option<Sender<Bag>>,
    // tracks upload job from the sender channel
    uploads: Arc<Mutex<HashMap<String, UploadJob>>>,
    // tracks download jobs from the receiver channel
    downloads: Arc<Mutex<HashMap<String, DownloadJob>>>,

    storage: S,
    // store location
    dir: Option<String>,
}

impl<S> Upload<S>
where
    S: Storage,
{
    pub fn new(storage: S, dir: Option<String>) -> Self {
        Self {
            storage,
            uploads: Arc::new(Mutex::new(HashMap::default())),
            downloads: Arc::new(Mutex::new(HashMap::default())),
            ch: None,
            dir: dir,
        }
    }

    async fn send(&self, bag: Bag) -> Result<()> {
        self.ch
            .as_ref()
            .unwrap()
            .send(bag)
            .await
            .map_err(|_| anyhow::anyhow!("failed to push response"))
    }
    /// a local (sender) upload handler. all what this will do is validate the
    /// request and then send a negotiate request
    ///
    /// the negotiate request is needed only to validate that remote side is
    /// listening and actually accept file uploads.
    async fn local_upload(&self, request: &JsonOutgoingRequest) -> Result<()> {
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

        let open = UploadOpen {
            size: meta.len(),
            cmd: body.cmd.clone(),
        };

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
        let data = serde_json::to_vec(&open).context("failed to build request")?;

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

        self.send(bag).await
    }

    /// a utility function to send back responses to the local client (the one request the upload)
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

    async fn remote_request_negotiate(&self, request: &Envelope) -> Result<()> {
        let dir = match self.dir {
            Some(ref dir) => dir,
            None => bail!("upload is not enabled"),
        };

        let _body: UploadOpen =
            serde_json::from_slice(request.plain()).context("failed to read request body")?;

        // TODO: add check if size is acceptable
        let path = Path::new(dir);
        let file = fs::File::create(path.join(&request.uid))
            .await
            .context("failed to prepare a file for an upload")?;

        let mut downloads = self.downloads.lock().await;
        downloads.insert(request.uid.clone(), DownloadJob { output: file });

        let mut env = Envelope {
            uid: request.uid.clone(),
            destination: request.source.clone(),
            ..Default::default()
        };

        // mark as response
        env.mut_response();
        // send back
        self.send(Bag::one(env)).await
    }

    async fn remote_request_write(&self, request: &Envelope) -> Result<()> {
        let mut downloads = self.downloads.lock().await;

        let download = downloads
            .get_mut(&request.uid)
            .ok_or_else(|| anyhow::anyhow!("write operation for an upload that does not exist"))?;

        // TODO: if we failed to write a chunk, right now the sender will just keep
        // sending the rest of the chunks.
        // sending an error here `Err` will actually send a message back to the
        // sender but current code does not care and will keep sending the rest of the
        // chunks (more like a stream).
        // two possible solutions:
        // - on close, send a request to ask the sender to send back all missing parts
        // - on close send an error back that upload failed and file need to be send over.
        download
            .output
            .write_all(request.plain())
            .await
            .context("failed to write chunk")
    }

    async fn remote_request_close(&self, request: &Envelope) -> Result<()> {
        let mut downloads = self.downloads.lock().await;

        downloads
            .remove(&request.uid)
            .ok_or_else(|| anyhow::anyhow!("write operation for an upload that does not exist"))?;

        log::info!("upload {} complete", request.uid);
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
            FILE_NEGOTIATE => self.remote_request_negotiate(request).await,
            FILE_WRITE => self.remote_request_write(request).await,
            FILE_CLOSE => self.remote_request_close(request).await,
            _ => Err(anyhow::anyhow!("unknown command: {}", req.command)),
        }
    }

    /// handles responses from a remote peer
    async fn remote_response(&self, tracker: Backlog, response: &Envelope) {
        let mut uploads = self.uploads.lock().await;
        let upload = match uploads.get_mut(&tracker.uid) {
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

                if let Err(err) = self.local_start_upload(&tracker, upload).await {
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
                upload.state = UploadState::Uploading;
            }
            UploadState::Uploading => {
                // already uploading and shouldn't be here
                // but this is reserved in case the remote peer sent a message
                // regarding this upload to cancel or
            }
        }
    }

    async fn local_start_upload(&self, tracker: &Backlog, job: &UploadJob) -> Result<()> {
        let file = fs::File::open(&job.request.path)
            .await
            .context("failed to open file for reading")?;

        let meta = file.metadata().await.context("failed to get file state")?;
        let mut base = Envelope {
            uid: tracker.uid.clone(),
            destination: address(job.dst).into(),
            ..Default::default()
        };

        // set initial command to file.write
        // will be used by the generator to send the write
        // command and auto-fill the chunks
        let req = base.mut_request();
        req.command = FILE_WRITE.into();

        // TODO: the tracker can timeout, so we need to set a bigger ttl, or find another mechanism to
        // keep tracking of uploads ids.
        //
        // we also need to tell the other peer that the upload is complete. this can be done by sending
        // a last message with the file.commit message may be
        let bag = Bag::new(ChunkGenerator::new(base, file, meta.len())).backlog(tracker.clone());

        self.send(bag).await
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
            FILE_UPLOAD => self.local_upload(&request).await,
            _ => Err(anyhow::anyhow!("unknown command: {}", request.command)),
        };

        if let Err(err) = result {
            self.send_local(
                &request.reply_to,
                &request.reference,
                LocalResp::Error(format!("{:#}", err)),
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

            if let Err(err) = self.send(Bag::one(env)).await {
                log::error!("failed to send error message back to caller: {:#}", err);
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
    remaining: u64,
    buffer: Vec<u8>,
    ended: bool,
}

impl ChunkGenerator {
    fn new(base: Envelope, f: fs::File, size: u64) -> Self {
        Self {
            base,
            inner: BufReader::new(f),
            remaining: size,
            buffer: vec![0; FILE_CHUNK_SIZE],
            ended: false,
        }
    }

    async fn next_env(&mut self) -> Result<Option<Envelope>> {
        let len = self
            .inner
            .read(&mut self.buffer)
            .await
            .context("failed to read file")?;

        self.remaining -= len as u64;
        log::trace!("upload read {} bytes", len);

        // the iterator has already been exhausted
        if len == 0 && self.ended {
            return Ok(None);
        }

        let mut env = self.base.clone();
        if len == 0 {
            // and self.ended == false also
            let req = env.mut_request();
            req.command = FILE_CLOSE.into();
            self.ended = true;
        } else {
            env.set_plain(self.buffer[..len].into());
        }
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

    fn count_hint(&self) -> Option<u64> {
        return Some(self.remaining / FILE_CHUNK_SIZE as u64);
    }
}
