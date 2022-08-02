use super::data::AppData;
use super::errors::HandlerError;
use crate::twin::TwinDB;
use crate::types::UploadRequest;
use crate::{identity::Identity, storage::Storage, types::Message};
use anyhow::{Context, Result};
use futures::TryStreamExt;
use hyper::http::{header, Request};
use hyper::Body;
use mpart_async::server::{MultipartField, MultipartStream};
use std::fs::{remove_file, File};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub enum UploadConfig {
    Enabled(PathBuf),
    Disabled,
}

impl UploadConfig {
    pub fn join<P: AsRef<Path>>(&self, p: P) -> Result<PathBuf> {
        match self {
            Self::Enabled(c) => Ok(c.join(p)),
            Self::Disabled => bail!("upload is not enabled"),
        }
    }

    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }
}

#[derive(Clone)]
pub struct UploadHandler<S: Storage, I: Identity, D: TwinDB> {
    data: AppData<S, I, D>,
}

impl<'a, S, I, D> UploadHandler<S, I, D>
where
    S: Storage,
    I: Identity,
    D: TwinDB,
{
    pub fn new(data: AppData<S, I, D>) -> Self {
        Self { data }
    }

    fn get_header(&self, request: &Request<Body>, key: &str) -> String {
        match request.headers().get(key).and_then(|val| val.to_str().ok()) {
            Some(value) => value.to_string(),
            None => "".to_string(),
        }
    }

    async fn verify_request(&self, request: &Request<Body>) -> Result<(UploadRequest, u32)> {
        let source = self.get_header(request, "rmb-source-id").parse::<u32>()?;
        let timestamp = self.get_header(request, "rmb-timestamp").parse::<u64>()?;
        let signature = self.get_header(request, "rmb-signature");

        let upload = UploadRequest::new(
            PathBuf::from(""),
            self.get_header(request, "rmb-upload-cmd"),
        );

        // getting sender twin
        let sender_twin = self
            .data
            .twin_db
            .get_twin(source)
            .await
            .context("failed to get source twin")?;

        let sender_twin = match sender_twin {
            Some(twin) => twin,
            None => bail!("source twin not found"),
        };

        upload.verify(&sender_twin.account, timestamp, source, signature)?;
        Ok((upload, source))
    }

    async fn send_message_to_processor(&self, upload: &UploadRequest, source: u32, path: &Path) {
        let mut msg = Message::default();

        let dst = vec![source];
        msg.source = self.data.twin;
        msg.destination = dst;
        msg.command = upload.cmd.clone();

        let path_str = path.to_string_lossy();
        msg.data = base64::encode(&path_str.to_string());
        msg.stamp();

        log::debug!("sending to upload command: {}", msg.command);

        if let Err(err) = self
            .data
            .storage
            .run(msg)
            .await
            .context("can not run upload command")
        {
            log::error!("failed to run upload command: {}", err);
        }
    }

    fn get_multipart_stream(
        &self,
        request: &'a mut Request<Body>,
    ) -> Result<MultipartStream<&'a mut Body, hyper::Error>> {
        let m = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|val| val.to_str().ok())
            .and_then(|val| val.parse::<mime::Mime>().ok())
            .ok_or_else(|| anyhow!("cannot get mime type"))?;

        let boundary = m
            .get_param("boundary")
            .map(|v| v.to_string())
            .ok_or_else(|| anyhow!("cannot get content boundary"))?;

        let body = request.body_mut();
        let stream = MultipartStream::new(boundary, body);

        Ok(stream)
    }

    async fn process_multipart_field(
        &self,
        field: &mut MultipartField<&'a mut Body, hyper::Error>,
    ) -> Result<PathBuf> {
        // we always generate a uuid as a filename
        let path = self
            .data
            .upload_config
            .join(uuid::Uuid::new_v4().to_string())?;

        let mut file = File::create(&path).with_context(|| "cannot create the file")?;
        while let Ok(Some(bytes)) = field.try_next().await {
            file.write_all(&bytes)
                .with_context(|| "cannot write data to file")
                .or_else(|err| {
                    log::debug!("cleaning up {:?}", &path);
                    remove_file(&path).with_context(|| "cannot clean up written file")?;
                    Err(err)
                })?;
        }

        Ok(path)
    }

    pub async fn handle(&self, mut request: Request<Body>) -> Result<(), HandlerError> {
        // first verify this upload request
        let (upload, source) = match self.verify_request(&request).await {
            Ok(ret) => ret,
            Err(err) => return Err(HandlerError::BadRequest(err)),
        };

        let mut stream = match self.get_multipart_stream(&mut request) {
            Ok(s) => s,
            Err(err) => return Err(HandlerError::BadRequest(err)),
        };

        while let Ok(Some(mut field)) = stream.try_next().await {
            match self.process_multipart_field(&mut field).await {
                Ok(path) => self.send_message_to_processor(&upload, source, &path).await,
                Err(err) => {
                    log::debug!("error processing multipart field: {}", err.to_string());
                    return Err(HandlerError::InternalError(err));
                }
            }
        }

        Ok(())
    }
}
