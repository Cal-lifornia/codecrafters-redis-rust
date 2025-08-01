use std::{
    borrow::Borrow,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{db::RedisDatabase, redis::RedisResult, resp::Resp, RedisError};

enum Command {
    Echo(String),
    Ping,
    Get(String),
    Set(String, String, Option<Duration>, bool),
}

impl Command {
    fn run_command(&self, db: Arc<RedisDatabase>) -> RedisResult {
        match self {
            Command::Echo(contents) => Ok(Resp::BulkString(contents.to_string())),
            Command::Ping => Ok(Resp::SimpleString("PONG".to_string())),
            Command::Set(key, value, expiry, keep_ttl) => {
                match db.set(key, value, *expiry, *keep_ttl) {
                    Ok(_) => Ok(Resp::SimpleString("OK".to_string())),
                    Err(err) => Err(err.into()),
                }
            }
            Command::Get(key) => match db.get(key)? {
                Some(val) => Ok(Resp::SimpleString(val)),
                None => Ok(Resp::NullBulkString),
            },
        }
    }
}

pub fn parse_array_command(input: &[Resp], db: Arc<RedisDatabase>) -> RedisResult {
    let mut inputs: Vec<&str> = vec![];
    for arg in input {
        if let Resp::BulkString(val) = arg {
            inputs.push(val)
        } else {
            return Err(RedisError::InvalidInput);
        }
    }

    match inputs[0].to_lowercase().as_str() {
        "echo" => {
            let args = &inputs[1..];
            if args.len() == 1 {
                Command::Echo(args[0].to_string()).run_command(db)
            } else {
                Err(RedisError::InvalidInput)
            }
        }
        "ping" => Command::Ping.run_command(db),
        "get" => {
            let args = &inputs[1..];

            if !args.len() > 1 {
                Command::Get(args[0].to_string()).run_command(db)
            } else {
                Err(RedisError::InvalidInput)
            }
        }
        "set" => {
            let args = &inputs[1..];
            parse_set_command(args)?.run_command(db)
        }
        _ => Err(RedisError::InvalidCommand(inputs[0].to_string())),
    }
}

fn parse_set_command(args: &[&str]) -> Result<Command, RedisError> {
    match args.len() {
        2 => Ok(Command::Set(
            args[0].to_string(),
            args[1].to_string(),
            None,
            false,
        )),
        3..4 => {
            let (key, val, expiry_opt) = (args[0], args[1], args[2]);

            if expiry_opt.to_lowercase().as_str() == "keepttl" {
                return Ok(Command::Set(key.to_string(), val.to_string(), None, true));
            }
            if args.len() != 4 {
                return Err(RedisError::InvalidInput);
            }
            let expiry_time = args[3];
            let expiry = match expiry_opt.to_lowercase().as_str() {
                "ex" => Duration::from_secs(expiry_time.parse::<u64>()?),
                "px" => Duration::from_millis(expiry_time.parse::<u64>()?),
                "exat" => Duration::from_secs(expiry_time.parse::<u64>()?),
                "pxat" => Duration::from_millis(expiry_time.parse::<u64>()?),
                _ => return Err(RedisError::InvalidCommand(expiry_opt.to_string())),
            };

            Ok(Command::Set(
                key.to_string(),
                val.to_string(),
                Some(expiry),
                false,
            ))
        }
        _ => Err(RedisError::InvalidInput),
    }
}
