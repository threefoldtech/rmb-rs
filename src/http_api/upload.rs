use super::data::AppData;
use super::errors::HandlerError;
use crate::twin::TwinDB;
use crate::types::UploadPayload;
use crate::{identity::Identity, storage::Storage, types::Message};
use anyhow::{Context, Result};
use futures::TryStreamExt;
use hyper::http::{header, Request};
use hyper::Body;
use mpart_async::server::{MultipartField, MultipartStream};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct UploadConfig {
    pub enabled: bool,
    pub upload_dir: PathBuf,
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
        Self { data: data }
    }

    fn get_header(&self, request: &Request<Body>, key: &str) -> String {
        match request.headers().get(key).and_then(|val| val.to_str().ok()) {
            Some(value) => value.to_string(),
            None => "".to_string(),
        }
    }

    fn verify_request(&self, request: &Request<Body>) -> Result<UploadPayload> {
        let source = self.get_header(request, "rmb-source-id").parse::<u32>()?;
        let timestamp = self.get_header(request, "rmb-timestamp").parse::<u64>()?;

        let payload = UploadPayload::new(
            PathBuf::from(""),
            self.get_header(request, "rmb-upload-cmd"),
            source,
            timestamp,
            self.get_header(request, "rmb-signature"),
        );

        payload.verify(&self.data.identity)?;
        Ok(payload)
    }

    async fn send_message_to_processor(&self, payload: &UploadPayload) {
        let mut msg = Message::default();

        let dst = vec![payload.source];
        msg.source = self.data.twin;
        msg.destination = dst;
        msg.command = payload.cmd.clone();

        let path_str = payload.path.to_string_lossy();
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
        payload: &mut UploadPayload,
        field: &mut MultipartField<&'a mut Body, hyper::Error>,
    ) -> Result<()> {
        // if a file is provided, we always generate a uuid for as filename
        if let Ok(_) = field.filename() {
            let path = self
                .data
                .upload_config
                .upload_dir
                .join(format!("{}", uuid::Uuid::new_v4()));

            payload.path = path.clone();
            let mut file = File::create(&path).with_context(|| "cannot create the file")?;
            while let Ok(Some(bytes)) = field.try_next().await {
                file.write_all(&bytes)
                    .with_context(|| "cannot write data to file")?;
            }

            self.send_message_to_processor(payload).await;
        }

        Ok(())
    }

    pub async fn handle(&self, mut request: Request<Body>) -> Result<(), HandlerError> {
        // first verify this upload request
        let mut payload = match self.verify_request(&request) {
            Ok(p) => p,
            Err(err) => return Err(HandlerError::BadRequest(err)),
        };

        let mut stream = match self.get_multipart_stream(&mut request) {
            Ok(s) => s,
            Err(err) => return Err(HandlerError::BadRequest(err)),
        };

        while let Ok(Some(mut field)) = stream.try_next().await {
            if let Err(err) = self.process_multipart_field(&mut payload, &mut field).await {
                log::debug!("error processing multipart field: {}", err.to_string());
                return Err(HandlerError::InternalError(err));
            }
        }

        Ok(())
    }
}
