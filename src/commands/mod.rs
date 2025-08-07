use thiserror::Error;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::oneshot,
};

mod set;
use set::*;

use crate::{context::Context, db::DatabaseError, resp::Resp};

#[derive(Debug)]
pub enum RedisCommand {
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
    Blpop {
        key: String,
        count: usize,
        responder: Responder<Option<String>>,
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
    IOError(#[from] std::io::Error),
    #[error("{0}")]
    SendError(String),
    #[error("{0}")]
    DBError(#[from] DatabaseError),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for CommandError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::SendError(value.to_string())
    }
}

pub type Responder<T> = oneshot::Sender<Result<T, DatabaseError>>;

pub async fn parse_array_command<Writer>(
    out: &mut Writer,
    input: Vec<Resp>,
    ctx: &Context,
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    let mut inputs: Vec<String> = vec![];
    for arg in input.clone() {
        if let Resp::BulkString(val) = arg {
            inputs.push(val);
        } else {
            out.write_all(&Resp::SimpleString(CommandError::InvalidInput.to_string()).to_bytes())
                .await
                .unwrap();
            return Ok(());
        }
    }

    let (command, args) = inputs.split_first().unwrap();

    match command.to_lowercase().as_str() {
        "echo" => {
            if args.len() == 1 {
                out.write_all(&Resp::BulkString(args[0].to_string()).to_bytes())
                    .await?;
            } else {
                out.write_all(
                    &Resp::SimpleError(CommandError::WrongNumArgs("echo".to_string()).to_string())
                        .to_bytes(),
                )
                .await?;
            }
        }
        "ping" => {
            out.write_all(&Resp::SimpleString("PONG".to_string()).to_bytes())
                .await?;
        }
        "get" => {
            if !args.len() > 1 {
                let (responder, receiver) = oneshot::channel();
                ctx.db_sender
                    .send(RedisCommand::Get {
                        key: args[0].clone(),
                        responder,
                    })
                    .await?;
                match receiver.await.unwrap()? {
                    Some(val) => {
                        out.write_all(&Resp::SimpleString(val).to_bytes()).await?;
                    }
                    None => {
                        out.write_all(&Resp::NullBulkString.to_bytes()).await?;
                    }
                };
            } else {
                out.write_all(
                    &Resp::simple_error(CommandError::WrongNumArgs("get".to_string())).to_bytes(),
                )
                .await?;
            }
        }
        "set" => {
            let (responder, receiver) = oneshot::channel();
            ctx.db_sender
                .send(RedisCommand::Set {
                    args: SetCommandArgs::parse(args.to_vec())?,
                    responder,
                })
                .await?;
            receiver.await.unwrap()?;
            out.write_all(&Resp::BulkString("OK".to_string()).to_bytes())
                .await?;
        }
        "rpush" => {
            let (responder, receiver) = oneshot::channel();
            ctx.db_sender
                .send(RedisCommand::Rpush {
                    key: args[0].clone(),
                    values: args[1..].to_vec(),
                    responder,
                })
                .await?;
            out.write_all(&Resp::Integer(receiver.await.unwrap()?).to_bytes())
                .await?;
        }
        "lpush" => {
            let (responder, receiver) = oneshot::channel();
            ctx.db_sender
                .send(RedisCommand::Lpush {
                    key: args[0].clone(),
                    values: args[1..].to_vec(),
                    responder,
                })
                .await?;
            out.write_all(&Resp::Integer(receiver.await.unwrap()?).to_bytes())
                .await?;
        }
        "lrange" => {
            if args.len() > 3 {
                return Err(CommandError::WrongNumArgs("lpush".to_string()));
            }
            let (responder, receiver) = oneshot::channel();

            ctx.db_sender
                .send(RedisCommand::Lrange {
                    key: args[0].clone(),
                    start: args[1].parse()?,
                    end: args[2].parse()?,
                    responder,
                })
                .await?;
            out.write_all(&Resp::StringArray(receiver.await.unwrap()?).to_bytes())
                .await?;
        }
        "llen" => {
            if args.len() > 2 {
                return Err(CommandError::WrongNumArgs("llen".to_string()));
            }

            let (responder, receiver) = oneshot::channel();
            ctx.db_sender
                .send(RedisCommand::Llen {
                    key: args[0].clone(),
                    responder,
                })
                .await?;

            out.write_all(&Resp::Integer(receiver.await.unwrap()?).to_bytes())
                .await?;
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

            ctx.db_sender
                .send(RedisCommand::Lpop {
                    key: args[0].clone(),
                    count,
                    responder,
                })
                .await?;

            match receiver.await.unwrap()? {
                Some(list) => {
                    if list.len() == 1 {
                        out.write_all(&Resp::BulkString(list[0].clone()).to_bytes())
                            .await?
                    } else {
                        out.write_all(&Resp::StringArray(list).to_bytes()).await?
                    }
                }
                None => out.write_all(&Resp::NullBulkString.to_bytes()).await?,
            };
        }
        "blpop" => {
            if args.len() > 2 {
                out.write_all(
                    &Resp::simple_error(CommandError::WrongNumArgs("blpop".into())).to_bytes(),
                )
                .await?;
            } else {
                let (responder, receiver) = oneshot::channel();

                let count = if args.len() == 2 {
                    args[1].parse::<usize>()?
                } else {
                    0
                };

                ctx.db_sender
                    .send(RedisCommand::Blpop {
                        key: args[0].clone(),
                        count,
                        responder,
                    })
                    .await?;
                if let Some(results) = receiver.await.unwrap()? {
                    out.write_all(&Resp::BulkString(results).to_bytes()).await?;
                } else {
                    out.write_all(
                        &Resp::SimpleError("failed to get results".to_string()).to_bytes(),
                    )
                    .await?;
                }
            }
        }

        _ => {
            out.write_all(
                &Resp::simple_error(CommandError::InvalidCommand(inputs[0].to_string())).to_bytes(),
            )
            .await?;
        }
    };
    Ok(())
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
