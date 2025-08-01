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
    RespError(#[from] RespError),
    #[error("{0}")]
    DBError(#[from] DatabaseError),
    #[error("error parsing input {0}")]
    IntParseError(#[from] std::num::ParseIntError),
    #[error("error parsing input {0}")]
    StringParseError(#[from] std::string::ParseError),
    #[error("{0}")]
    Other(Box<dyn std::error::Error>),
    #[error("{0}")]
    Unknown(String),
}
