use std::sync::Arc;

use anyhow::{Ok, Result};
use hyper::{Body, Method, Request, Response, StatusCode};

use crate::{storage::Storage, types::Identity};

use super::{handles::*, AppData};

static NOTFOUND: &[u8] = b"Not Found";

pub async fn routes<S: Storage, I: Identity>(
    req: Request<Body>,
    data: Arc<AppData<S, I>>,
) -> Result<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/rmb-remote") => rmb_remote(req, data.clone()).await,
        (&Method::POST, "/rmb-reply") => rmb_reply(req, data.clone()).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(NOTFOUND.into())
            .unwrap()),
    }
}
