use std::{sync::Arc, time::Duration};

use thiserror::Error;

use crate::{
    db::{DatabaseError, RedisDatabase},
    resp::Resp,
};

pub type CommandResult = Result<Resp, CommandError>;

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("invalid command {0}")]
    InvalidCommand(String),
    #[error("invalid input")]
    InvalidInput,
    #[error("invalid number of arguments for {0}")]
    WrongNumArgs(String),
    #[error("error parsing input {0}")]
    IntParseError(#[from] std::num::ParseIntError),
    #[error("error parsing input {0}")]
    StringParseError(#[from] std::string::ParseError),
    #[error("{0}")]
    DBError(#[from] DatabaseError),
}

#[derive(Debug, PartialEq, Eq)]
enum Command<'a> {
    Echo(&'a str),
    Ping,
    Get(&'a str),
    Set(&'a str, &'a str, Option<Duration>, bool),
    Rpush(&'a str, &'a [String]),
    Lpush(&'a str, &'a [String]),
    Lrange(&'a str, i32, i32),
    Llen(&'a str),
    Lpop(&'a str, Option<usize>),
}

impl<'a> Command<'a> {
    fn run_command(&self, db: Arc<RedisDatabase>) -> CommandResult {
        match self {
            Command::Echo(contents) => Ok(Resp::BulkString(contents.to_string())),
            Command::Ping => Ok(Resp::SimpleString("PONG".to_string())),
            Command::Set(key, value, expiry, keep_ttl) => {
                match db.set_string(key, value, *expiry, *keep_ttl) {
                    Ok(_) => Ok(Resp::SimpleString("OK".to_string())),
                    Err(err) => Err(err.into()),
                }
            }
            Command::Get(key) => match db.get_string(key)? {
                Some(val) => Ok(Resp::SimpleString(val)),
                None => Ok(Resp::NullBulkString),
            },
            Command::Rpush(key, values) => Ok(Resp::Integer(db.push_list(key, values)?)),
            Command::Lpush(key, values) => {
                Ok(Resp::Integer(db.prepend_list(key, values.to_vec())?))
            }
            Command::Lrange(key, start, end) => {
                Ok(Resp::StringArray(db.read_list(key, *start, *end)?))
            }
            Command::Llen(key) => Ok(Resp::Integer(db.get_list_length(key)?)),
            Command::Lpop(key, count) => {
                let result = db.pop_first_list(key, *count)?;
                if let Some(list) = result {
                    if list.len() == 1 {
                        Ok(Resp::BulkString(list[0].clone()))
                    } else {
                        Ok(Resp::StringArray(list))
                    }
                } else {
                    Ok(Resp::NullBulkString)
                }
            }
        }
    }
}

pub fn parse_array_command(input: &[Resp], db: Arc<RedisDatabase>) -> CommandResult {
    let mut inputs: Vec<&str> = vec![];
    for arg in input {
        if let Resp::BulkString(val) = arg {
            inputs.push(val)
        } else {
            return Err(CommandError::InvalidInput);
        }
    }

    match inputs[0].to_lowercase().as_str() {
        "echo" => {
            let args = &inputs[1..];
            if args.len() == 1 {
                Command::Echo(args[0]).run_command(db)
            } else {
                Err(CommandError::WrongNumArgs("echo".to_string()))
            }
        }
        "ping" => Command::Ping.run_command(db),
        "get" => {
            let args = &inputs[1..];

            if !args.len() > 1 {
                Command::Get(args[0]).run_command(db)
            } else {
                Err(CommandError::WrongNumArgs("get".to_string()))
            }
        }
        "set" => {
            let args = &inputs[1..];
            parse_set_command(args)?.run_command(db)
        }
        "rpush" => Command::Rpush(
            inputs[1],
            &inputs[2..]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
        )
        .run_command(db),
        "lpush" => Command::Lpush(
            inputs[1],
            &inputs[2..]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
        )
        .run_command(db),
        "lrange" => {
            let args = &inputs[1..];
            if args.len() > 3 {
                return Err(CommandError::WrongNumArgs("lpush".to_string()));
            }

            Command::Lrange(args[0], args[1].parse()?, args[2].parse()?).run_command(db)
        }
        "llen" => Command::Llen(inputs[1]).run_command(db),
        "lpop" => {
            let args = &inputs[1..];
            if args.len() > 2 {
                return Err(CommandError::WrongNumArgs("lpop".to_string()));
            }

            let count: Option<usize> = if args.len() == 2 {
                Some(args[1].parse::<usize>()?)
            } else {
                None
            };

            Command::Lpop(args[0], count).run_command(db)
        }

        _ => Err(CommandError::InvalidCommand(inputs[0].to_string())),
    }
}

fn parse_set_command<'a>(args: &'a [&'a str]) -> Result<Command<'a>, CommandError> {
    match args.len() {
        2 => Ok(Command::Set(args[0], args[1], None, false)),
        3 | 4 => {
            let (key, val, expiry_opt) = (args[0], args[1], args[2]);

            if expiry_opt.to_lowercase().as_str() == "keepttl" {
                return Ok(Command::Set(key, val, None, true));
            }
            if args.len() != 4 {
                return Err(CommandError::InvalidInput);
            }
            let expiry_time = args[3];
            let expiry = match expiry_opt.to_lowercase().as_str() {
                "ex" => Duration::from_secs(expiry_time.parse::<u64>()?),
                "px" => Duration::from_millis(expiry_time.parse::<u64>()?),
                "exat" => Duration::from_secs(expiry_time.parse::<u64>()?),
                "pxat" => Duration::from_millis(expiry_time.parse::<u64>()?),
                _ => return Err(CommandError::InvalidCommand(expiry_opt.to_string())),
            };

            Ok(Command::Set(key, val, Some(expiry), false))
        }
        _ => Err(CommandError::WrongNumArgs("set".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_set_command() {
        // let test_commands = &[
        //     vec![
        //         Resp::BulkString("mango".to_string()),
        //         Resp::BulkString("blueberry".to_string()),
        //     ],
        //     vec![
        //         Resp::BulkString("mango".to_string()),
        //         Resp::BulkString("blueberry".to_string()),
        //         Resp::BulkString("px".to_string()),
        //         Resp::BulkString("100".to_string()),
        //     ],
        // ];
        let test_commands = &[
            vec!["mango", "blueberry"],
            vec!["mango", "blueberry", "px", "100"],
        ];

        let expected = &[
            Command::Set("mango", "blueberry", None, false),
            Command::Set(
                "mango",
                "blueberry",
                Some(Duration::from_millis(100)),
                false,
            ),
        ];

        test_commands
            .iter()
            .zip(expected.iter())
            .for_each(|(command, expecting)| {
                let result = parse_set_command(command).unwrap();
                assert_eq!(*expecting, result)
            })
    }
}
