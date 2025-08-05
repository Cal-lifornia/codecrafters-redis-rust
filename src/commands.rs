use std::{sync::Arc, time::Duration};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

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

#[derive(Debug)]
pub enum RedisCommand<'a> {
    Echo(&'a str),
    Ping,
    Get {
        key: &'a str,
        responder: Responder<Option<String>>,
    },
    Set {
        key: &'a str,
        value: &'a str,
        expiry: Option<Duration>,
        keep_ttl: bool,
        responder: Responder<()>,
    },
    Rpush {
        key: &'a str,
        values: &'a [&'a str],
        responder: Responder<i32>,
    },
    Lpush {
        key: &'a str,
        values: &'a [&'a str],
        responder: Responder<i32>,
    },
    Lrange {
        key: &'a str,
        start: i32,
        end: i32,
        responder: Responder<Vec<String>>,
    },
    Llen {
        key: &'a str,
        responder: Responder<i32>,
    },
    Lpop {
        key: &'a str,
        count: Option<usize>,
        responder: Responder<Option<Vec<String>>>,
    },
}

pub type Responder<T> = oneshot::Sender<Result<T, DatabaseError>>;

impl<'a> RedisCommand<'a> {
    fn run_command(&self, sender: &mpsc::Sender<RedisCommand<'a>>) -> CommandResult {
        match self {
            RedisCommand::Echo(contents) => Ok(Resp::BulkString(contents.to_string())),
            RedisCommand::Ping => Ok(Resp::SimpleString("PONG".to_string())),
            RedisCommand::Set {
                key,
                value,
                expiry,
                keep_ttl,
                responder: resp,
            } => match db.set_string(key, value, *expiry, *keep_ttl) {
                Ok(_) => Ok(Resp::SimpleString("OK".to_string())),
                Err(err) => Err(err.into()),
            },
            RedisCommand::Get(key) => match db.get_string(key)? {
                Some(val) => Ok(Resp::SimpleString(val)),
                None => Ok(Resp::NullBulkString),
            },
            RedisCommand::Rpush(key, values) => Ok(Resp::Integer(db.push_list(key, values)?)),
            RedisCommand::Lpush(key, values) => Ok(Resp::Integer(db.prepend_list(key, values)?)),
            RedisCommand::Lrange(key, start, end) => {
                Ok(Resp::StringArray(db.read_list(key, *start, *end)?))
            }
            RedisCommand::Llen(key) => Ok(Resp::Integer(db.get_list_length(key)?)),
            RedisCommand::Lpop(key, count) => {
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

pub async fn parse_array_command<'a>(
    input: &[Resp],
    sender: mpsc::Sender<RedisCommand<'a>>,
) -> CommandResult {
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
                RedisCommand::Echo(args[0]).run_command(db)
            } else {
                Err(CommandError::WrongNumArgs("echo".to_string()))
            }
        }
        "ping" => RedisCommand::Ping.run_command(db),
        "get" => {
            let args = &inputs[1..];
            if !args.len() == 1 {
                let (responder, receiver) = oneshot::channel();
                sender
                    .send(RedisCommand::Get {
                        key: args[0],
                        responder,
                    })
                    .await
                    .unwrap();
                match receiver.await.unwrap() {
                    Ok(_) => Ok(Resp::BulkString("OK".to_string())),
                    Err(err) => Err(err.into()),
                }
            } else {
                Err(CommandError::WrongNumArgs("get".to_string()))
            }
        }
        "set" => {
            let args = &inputs[1..];
            parse_set_command(args)?
        }
        "rpush" => RedisCommand::Rpush(inputs[1], &inputs[2..]).run_command(db),
        "lpush" => RedisCommand::Lpush(inputs[1], &inputs[2..]).run_command(db),
        "lrange" => {
            let args = &inputs[1..];
            if args.len() > 3 {
                return Err(CommandError::WrongNumArgs("lpush".to_string()));
            }

            RedisCommand::Lrange(args[0], args[1].parse()?, args[2].parse()?).run_command(db)
        }
        "llen" => RedisCommand::Llen(inputs[1]).run_command(db),
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

            RedisCommand::Lpop(args[0], count).run_command(db)
        }

        _ => Err(CommandError::InvalidCommand(inputs[0].to_string())),
    }
}

fn parse_set_command<'a>(args: &'a [&'a str]) -> Result<RedisCommand<'a>, CommandError> {
    match args.len() {
        2 => Ok(RedisCommand::Set {
            key: args[0],
            value: args[1],
            expiry: None,
            keep_ttl: false,
            responder: _,
        }),
        3 | 4 => {
            let (key, val, expiry_opt) = (args[0], args[1], args[2]);

            if expiry_opt.to_lowercase().as_str() == "keepttl" {
                return Ok(RedisCommand::Set(key, val, None, true));
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

            Ok(RedisCommand::Set(key, val, Some(expiry), false))
        }
        _ => Err(CommandError::WrongNumArgs("set".to_string())),
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_parse_set_command() {
//         // let test_commands = &[
//         //     vec![
//         //         Resp::BulkString("mango".to_string()),
//         //         Resp::BulkString("blueberry".to_string()),
//         //     ],
//         //     vec![
//         //         Resp::BulkString("mango".to_string()),
//         //         Resp::BulkString("blueberry".to_string()),
//         //         Resp::BulkString("px".to_string()),
//         //         Resp::BulkString("100".to_string()),
//         //     ],
//         // ];
//         let test_commands = &[
//             vec!["mango", "blueberry"],
//             vec!["mango", "blueberry", "px", "100"],
//         ];

//         let expected = &[
//             RedisCommand::Set("mango", "blueberry", None, false),
//             RedisCommand::Set(
//                 "mango",
//                 "blueberry",
//                 Some(Duration::from_millis(100)),
//                 false,
//             ),
//         ];

//         test_commands
//             .iter()
//             .zip(expected.iter())
//             .for_each(|(command, expecting)| {
//                 let result = parse_set_command(command).unwrap();
//                 assert_eq!(*expecting, result)
//             })
//     }
// }
