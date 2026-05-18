//! Handler error types and HTTP status mapping.
//!
//! Each variant maps to a specific status code that matches the
//! existing Python Lambda's response codes, so the frontend's error
//! handling continues to work unchanged.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum HandlerError {
    /// Origin or destination missing or malformed in the request body.
    #[error("invalid request: {0}")]
    BadRequest(String),

    /// API key header missing or didn't match.
    #[error("unauthorized")]
    Unauthorized,

    /// No path exists between origin and destination in the graph.
    /// 422 is what the Python implementation returns; semantically
    /// the request was well-formed but routing was impossible.
    #[error("{0}")]
    NoRoute(String),

    /// Graph couldn't be loaded from S3 or API key couldn't be
    /// fetched from SSM. 503 = service temporarily unavailable.
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    /// Anything we didn't anticipate. Logged in full but 500'd to the
    /// caller without internal details.
    #[error("internal error: {0}")]
    Internal(String),
}

impl HandlerError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::BadRequest(_) => 400,
            Self::Unauthorized => 401,
            Self::NoRoute(_) => 422,
            Self::ServiceUnavailable(_) => 503,
            Self::Internal(_) => 500,
        }
    }

    /// Body string for the response. Internal errors get a generic
    /// message so we don't leak implementation details.
    pub fn body_message(&self) -> String {
        match self {
            Self::Internal(_) => "Routing failed".to_string(),
            other => other.to_string(),
        }
    }
}
