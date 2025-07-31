use std::{borrow::Borrow, sync::Arc};

use crate::{db::RedisDatabase, redis::RedisResult, resp::Resp, RedisError};

enum Command {
    Echo(Resp),
    Ping,
    Get(String),
    Set(String, String),
}
impl Command {
    fn run_command(&self, db: Arc<RedisDatabase>) -> RedisResult {
        match self {
            Command::Echo(contents) => Ok(contents.clone()),
            Command::Ping => Ok(Resp::SimpleString("PONG".to_string())),
            Command::Set(key, value) => match db.set(key, value) {
                Ok(_) => Ok(Resp::SimpleString("OK".to_string())),
                Err(err) => Err(err.into()),
            },
            Command::Get(key) => match db.get(key)? {
                Some(val) => Ok(Resp::SimpleString(val)),
                None => Ok(Resp::NullBulkString),
            },
        }
    }
}

pub fn parse_array_command(input: &[Resp], db: Arc<RedisDatabase>) -> RedisResult {
    if let Resp::BulkString(command) = input[0].borrow() {
        match command.to_lowercase().as_str() {
            "echo" => {
                let args = &input[1..];
                if args.len() == 1 {
                    Command::Echo(args[0].clone()).run_command(db)
                } else {
                    Command::Echo(Resp::Array(input[1..].to_vec())).run_command(db)
                }
            }
            "ping" => Command::Ping.run_command(db),
            "get" => {
                let args = &input[1..];

                if !args.len() > 1 {
                    if let Resp::BulkString(key) = args[0].borrow() {
                        Command::Get(key.to_string()).run_command(db)
                    } else {
                        Err(RedisError::InvalidInput)
                    }
                } else {
                    Err(RedisError::InvalidInput)
                }
            }
            "set" => {
                let args = &input[1..];
                if !args.len() != 2 {
                    if let (Resp::BulkString(key), Resp::BulkString(val)) = (&args[0], &args[1]) {
                        Command::Set(key.to_string(), val.to_string()).run_command(db)
                    } else {
                        Err(RedisError::InvalidInput)
                    }
                } else {
                    Err(RedisError::InvalidInput)
                }
            }
            _ => Err(RedisError::InvalidCommand(command.to_string())),
        }
    } else {
        Err(RedisError::InvalidInput)
    }
}
