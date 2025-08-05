use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

mod set;
pub use set::*;

use crate::{db::DatabaseError, resp::Resp};

#[derive(Debug)]
pub enum RedisCommand<'a> {
    Echo(&'a str),
    Ping,
    Get {
        key: String,
        responder: Responder<Option<String>>,
    },
    Set {
        args: SetCommandArgs,
        responder: Responder<()>,
    },
    Rpush {
        key: String,
        values: Vec<String>,
        responder: Responder<i32>,
    },
    Lpush {
        key: String,
        values: Vec<String>,
        responder: Responder<i32>,
    },
    Lrange {
        key: String,
        start: i32,
        end: i32,
        responder: Responder<Vec<String>>,
    },
    Llen {
        key: String,
        responder: Responder<i32>,
    },
    Lpop {
        key: String,
        count: Option<usize>,
        responder: Responder<Option<Vec<String>>>,
    },
}
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

pub type Responder<T> = oneshot::Sender<Result<T, DatabaseError>>;

pub async fn parse_array_command<'a>(
    input: Vec<Resp>,
    sender: mpsc::Sender<RedisCommand<'a>>,
) -> CommandResult {
    let mut inputs: Vec<String> = vec![];
    for arg in input.clone() {
        if let Resp::BulkString(val) = arg {
            inputs.push(val);
        } else {
            return Err(CommandError::InvalidInput);
        }
    }

    let (command, args) = inputs.split_first().unwrap();

    match command.to_lowercase().as_str() {
        "echo" => {
            if args.len() == 1 {
                Ok(Resp::BulkString(args[0].to_string()))
            } else {
                Err(CommandError::WrongNumArgs("echo".to_string()))
            }
        }
        "ping" => Ok(Resp::SimpleString("PONG".to_string())),
        "get" => {
            if !args.len() == 1 {
                let (responder, receiver) = oneshot::channel();
                sender
                    .send(RedisCommand::Get {
                        key: args[0].clone(),
                        responder,
                    })
                    .await
                    .unwrap();
                match receiver.await.unwrap()? {
                    Some(val) => Ok(Resp::SimpleString(val)),
                    None => Ok(Resp::NullBulkString),
                }
            } else {
                Err(CommandError::WrongNumArgs("get".to_string()))
            }
        }
        "set" => {
            let (responder, receiver) = oneshot::channel();
            sender
                .send(RedisCommand::Set {
                    args: SetCommandArgs::parse(args.to_vec())?,
                    responder,
                })
                .await
                .unwrap();
            receiver.await.unwrap()?;

            Ok(Resp::BulkString("OK".to_string()))
        }
        "rpush" => {
            let (responder, receiver) = oneshot::channel();
            sender
                .send(RedisCommand::Rpush {
                    key: args[0].clone(),
                    values: args[1..].to_vec(),
                    responder,
                })
                .await
                .unwrap();
            Ok(Resp::Integer(receiver.await.unwrap()?))
        }
        "lpush" => {
            let (responder, receiver) = oneshot::channel();
            sender
                .send(RedisCommand::Lpush {
                    key: args[0].clone(),
                    values: args[2..].to_vec(),
                    responder,
                })
                .await
                .unwrap();
            Ok(Resp::Integer(receiver.await.unwrap()?))
        }
        "lrange" => {
            if args.len() > 3 {
                return Err(CommandError::WrongNumArgs("lpush".to_string()));
            }
            let (responder, receiver) = oneshot::channel();

            sender
                .send(RedisCommand::Lrange {
                    key: args[0].clone(),
                    start: args[1].parse()?,
                    end: args[2].parse()?,
                    responder,
                })
                .await
                .unwrap();
            Ok(Resp::StringArray(receiver.await.unwrap()?))
        }
        "llen" => {
            if args.len() > 2 {
                return Err(CommandError::WrongNumArgs("llen".to_string()));
            }

            let (responder, receiver) = oneshot::channel();
            sender
                .send(RedisCommand::Llen {
                    key: args[0].clone(),
                    responder,
                })
                .await
                .unwrap();

            Ok(Resp::Integer(receiver.await.unwrap()?))
        }
        "lpop" => {
            if args.len() > 2 {
                return Err(CommandError::WrongNumArgs("lpop".to_string()));
            }
            let (responder, receiver) = oneshot::channel();

            let count: Option<usize> = if args.len() == 2 {
                Some(args[1].parse::<usize>()?)
            } else {
                None
            };

            sender
                .send(RedisCommand::Lpop {
                    key: args[0].clone(),
                    count,
                    responder,
                })
                .await
                .unwrap();

            match receiver.await.unwrap()? {
                Some(list) => {
                    if list.len() == 1 {
                        Ok(Resp::BulkString(list[0].clone()))
                    } else {
                        Ok(Resp::StringArray(list))
                    }
                }
                None => Ok(Resp::NullBulkString),
            }
        }

        _ => Err(CommandError::InvalidCommand(inputs[0].to_string())),
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
