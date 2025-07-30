use std::borrow::Borrow;

use anyhow::Result;
use thiserror::Error;

use crate::resp::{self, Resp, RespError};

enum Command {
    Echo(Resp),
    Ping,
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

pub fn handle_stream<S>(stream: &mut S) -> Result<()>
where
    S: std::io::Write + std::io::Read,
{
    let mut buf = [0; 512];
    loop {
        if let Ok(count) = stream.read(&mut buf) {
            if count == 0 {
                break;
            }
            parse_input(stream, &buf)?
        }
    }
    Ok(())
}

fn parse_input<S>(stream: &mut S, buf: &[u8]) -> Result<()>
where
    S: std::io::Write + std::io::Read,
{
    match resp::parse(buf) {
        Ok((input, _)) => match input {
            Resp::Array(contents) => match parse_array_command(&contents, stream) {
                Ok(_) => {}
                Err(err) => Resp::SimpleError(format!("ERR {}", err)).write_to_writer(stream)?,
            },
            // Resp::SimpleString(s) => match parse_simple_command(s, stream) {
            //     Ok(_) => {}
            //     Err(err) => Resp::SimpleError(format!("ERR {}", err)).write_to_writer(stream)?,
            // },
            _ => Resp::SimpleError("ERR invalid command".to_string()).write_to_writer(stream)?,
        },

        Err(e) => Resp::SimpleError(format!("ERR {}", e)).write_to_writer(stream)?,
    };
    Ok(())
}

impl Command {
    fn run_command<S>(&self, stream: &mut S) -> Result<(), RedisError>
    where
        S: std::io::Write + std::io::Read,
    {
        match self {
            Command::Echo(contents) => match contents.write_to_writer(stream) {
                Ok(_) => {}
                Err(err) => return Err(RedisError::Other(err.into())),
            },
            Command::Ping => match Resp::SimpleString("PONG".to_string()).write_to_writer(stream) {
                Ok(_) => {}
                Err(err) => return Err(RedisError::Other(err.into())),
            },
        };
        Ok(())
    }
}

fn parse_array_command<S>(input: &[Resp], stream: &mut S) -> Result<(), RedisError>
where
    S: std::io::Write + std::io::Read,
{
    if let Resp::BulkString(command) = input[0].borrow() {
        match command.to_lowercase().as_str() {
            "echo" => {
                let echos = &input[1..];
                if echos.len() == 1 {
                    Command::Echo(echos[0].clone()).run_command(stream)?;
                } else {
                    Command::Echo(Resp::Array(input[1..].to_vec())).run_command(stream)?;
                }
            }
            "ping" => Command::Ping.run_command(stream)?,
            _ => return Err(RedisError::InvalidCommand(command.to_string())),
        };
    };
    Ok(())
}

// fn parse_simple_command<S>(input: String, stream: &mut S) -> Result<(), RedisError>
// where
//     S: std::io::Write + std::io::Read,
// {
//     #[allow(clippy::single_match)]
//     match input.to_lowercase().as_str() {
//         "ping" => match Resp::SimpleString("PONG".to_string()).write_to_writer(stream) {
//             Ok(_) => {}
//             Err(err) => return Err(RedisError::Other(err.into())),
//         },
//         _ => {}
//     };
//     Ok(())
// }

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, io::Read};

    use super::*;

    #[test]
    fn test_handle_stream() {
        let test_input = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let expected = b"$3\r\nhey\r\n";

        let mut writer = VecDeque::new();

        parse_input(&mut writer, test_input).unwrap();

        let mut results = [0u8; 9];
        writer.read_exact(&mut results).unwrap();
        assert_eq!(results, *expected);
    }

    #[test]
    fn test_parse_command() {
        let test_input = vec![
            Resp::BulkString("echo".to_string()),
            Resp::BulkString("hey".to_string()),
        ];
        let expected = b"$3\r\nhey\r\n";
        let mut writer = VecDeque::new();

        parse_array_command(&test_input, &mut writer).unwrap();

        let mut results = [0u8; 9];
        writer.read_exact(&mut results).unwrap();
        assert_eq!(results, *expected);
    }

    // #[test]
    // fn test_parse_simple_command() {
    //     let test_input: Vec<String> =
    //         vec!["PING".to_string(), "ping".to_string(), "pInG".to_string()];
    //     let expected = b"+PONG\r\n";

    //     let mut writer = VecDeque::new();
    //     for input in test_input {
    //         parse_simple_command(input, &mut writer).unwrap();

    //         let mut results = [0u8; 7];
    //         writer.read_exact(&mut results).unwrap();
    //         assert_eq!(results, *expected);
    //         writer.clear()
    //     }
    // }
}
