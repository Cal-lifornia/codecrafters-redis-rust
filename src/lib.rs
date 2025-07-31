use resp::RespError;
use thiserror::Error;

pub mod commands;
pub mod db;
pub mod redis;
pub mod resp;

#[derive(Debug, Error)]
pub(crate) enum RedisError {
    #[error("invalid command {0}")]
    InvalidCommand(String),
    #[error("{0}")]
    ParserError(RespError),
    #[error("{0}")]
    Other(Box<dyn std::error::Error>),
}

impl From<RespError> for RedisError {
    fn from(value: RespError) -> Self {
        Self::ParserError(value)
    }
}
