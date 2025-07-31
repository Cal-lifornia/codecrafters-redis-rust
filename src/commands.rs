use std::borrow::Borrow;

use crate::{resp::Resp, RedisError};

enum Command {
    Echo(Resp),
    Ping,
    Get,
    Set,
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

pub fn parse_array_command<S>(input: &[Resp], stream: &mut S) -> Result<(), RedisError>
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
