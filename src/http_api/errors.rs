use thiserror::Error;

#[derive(Error, Debug)]
pub enum HandlerError {
    #[error("bad request: {0:#}")]
    BadRequest(anyhow::Error),

    #[error("unauthorized: {0:#}")]
    UnAuthorized(anyhow::Error),

    #[error("invalid destination twin {0}")]
    InvalidDestination(u32),

    #[error("invalid source twin {0}: {1:#}")]
    InvalidSource(u32, anyhow::Error),

    #[error("internal server error: {0:#}")]
    InternalError(#[from] anyhow::Error),
}
