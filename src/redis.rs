use std::{borrow::Borrow, fmt::format, io::Read, net::TcpStream};

use anyhow::{anyhow, Result};
use thiserror::Error;

use crate::resp::{self, Resp, RespError};

enum Command {
    Echo(Resp),
}

#[derive(Debug, Error)]
enum RedisError {
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

// impl<E> From<E> for RedisError
// where
//     E: Into<anyhow::Error>,
//     Result<(), E>: anyhow::Context<(), E>,
// {
//     fn from(value: E) -> Self {}
// }

pub fn handle_stream(stream: &mut TcpStream) -> Result<()> {
    let mut buf = [0; 512];
    loop {
        if let Ok(count) = stream.read(&mut buf) {
            if count == 0 {
                break;
            }
            match resp::parse(&buf) {
                Ok((input, _)) => match input {
                    Resp::Array(contents) => match parse_command(&contents, stream) {
                        Ok(_) => {}
                        Err(e) => {
                            Resp::SimpleError(format!("ERR {}", e)).write_to_writer(stream)?
                        }
                    },
                    _ => Resp::SimpleError("ERR invalid command".to_string())
                        .write_to_writer(stream)?,
                },

                Err(e) => Resp::SimpleError(format!("ERR {}", e)).write_to_writer(stream)?,
            };
        }
    }
    Ok(())
}

impl Command {
    fn run_command(&self, stream: &mut TcpStream) -> Result<(), RedisError> {
        match self {
            Command::Echo(contents) => match contents.write_to_writer(stream) {
                Ok(_) => {}
                Err(e) => return Err(RedisError::Other(e.into())),
            },
        };
        Ok(())
    }
}

fn parse_command(input: &[Resp], stream: &mut TcpStream) -> Result<(), RedisError> {
    if let Resp::BulkString(command) = input[0].borrow() {
        match command.to_lowercase().as_str() {
            "echo" => Command::Echo(Resp::Array(input[1..].to_vec())).run_command(stream)?,
            _ => return Err(RedisError::InvalidCommand(command.to_string())),
        };
    };
    Ok(())
}
