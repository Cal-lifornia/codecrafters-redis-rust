use resp::RespError;
use thiserror::Error;

use crate::commands::CommandError;

mod macros;
mod_pub!(commands db redis types resp replication);

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("{0}")]
    RespError(#[from] RespError),
    #[error("{0}")]
    CommandError(#[from] CommandError),
    #[error("{0}")]
    Other(Box<dyn std::error::Error>),
    #[error("{0}")]
    Unknown(String),
}
