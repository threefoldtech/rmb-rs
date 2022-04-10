use anyhow::{Ok, Result};
use hyper::{Body, Method, Request, Response, StatusCode};

use super::handles::*;

static NOTFOUND: &[u8] = b"Not Found";

pub async fn routes(req: Request<Body>) -> Result<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/rmb-remote") => rmb_remote(req).await,
        (&Method::POST, "/rmb-reply") => rmb_reply(req).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(NOTFOUND.into())
            .unwrap()),
    }
}
