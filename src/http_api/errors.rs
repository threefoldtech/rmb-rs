use hyper::http::StatusCode;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HandlerError {
    #[error("bad request: {0:#}")]
    BadRequest(anyhow::Error),

    #[error("unauthorized: {0:#}")]
    UnAuthorized(anyhow::Error),

    #[error("invalid destination twin {0}")]
    InvalidDestination(u32),

    #[error("invalid destination missing")]
    MissingDestination,

    #[error("invalid source twin {0}: {1:#}")]
    InvalidSource(u32, anyhow::Error),

    #[error("internal server error: {0:#}")]
    InternalError(#[from] anyhow::Error),
}

impl HandlerError {
    pub fn code(&self) -> StatusCode {
        match self {
            // the following ones are considered a bad request error
            HandlerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            HandlerError::InvalidDestination(_) => StatusCode::BAD_REQUEST,
            HandlerError::MissingDestination => StatusCode::BAD_REQUEST,

            // Unauthorized errors
            HandlerError::UnAuthorized(_) => StatusCode::UNAUTHORIZED,
            HandlerError::InvalidSource(_, _) => StatusCode::UNAUTHORIZED,

            // Internal server error
            HandlerError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
