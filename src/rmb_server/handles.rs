use std::sync::Arc;

use anyhow::Result;
use hyper::{Body, Request, Response};

use crate::{storage::Storage, types::Identity};

use super::AppData;

pub async fn rmb_remote<S: Storage, I: Identity>(
    _req: Request<Body>,
    _data: Arc<AppData<S, I>>,
) -> Result<Response<Body>> {
    Ok(Response::new("RmbRemote Endpoint".into()))
}

pub async fn rmb_reply<S: Storage, I: Identity>(
    _req: Request<Body>,
    _data: Arc<AppData<S, I>>,
) -> Result<Response<Body>> {
    Ok(Response::new("RmbReply Endpoint".into()))
}
