use anyhow::Result;
use hyper::{Body, Request, Response};

pub async fn rmb_remote(_req: Request<Body>) -> Result<Response<Body>> {
    Ok(Response::new("RmbRemote Endpoint".into()))
}

pub async fn rmb_reply(_req: Request<Body>) -> Result<Response<Body>> {
    Ok(Response::new("RmbReply Endpoint".into()))
}
