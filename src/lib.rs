use resp::RespError;
use thiserror::Error;

use crate::db::DatabaseError;

pub mod commands;
pub mod db;
pub mod redis;
pub mod resp;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("invalid command {0}")]
    InvalidCommand(String),
    #[error("invalid input")]
    InvalidInput,
    #[error("{0}")]
    ParserError(RespError),
    #[error("{0}")]
    DBError(DatabaseError),
    #[error("{0}")]
    Other(Box<dyn std::error::Error>),
    #[error("{0}")]
    Unknown(String),
}

impl From<RespError> for RedisError {
    fn from(value: RespError) -> Self {
        Self::ParserError(value)
    }
}

impl From<DatabaseError> for RedisError {
    fn from(value: DatabaseError) -> Self {
        Self::DBError(value)
    }
}
