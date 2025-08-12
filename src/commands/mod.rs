use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use thiserror::Error;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::oneshot,
    time::timeout,
};

mod set;
use set::*;

use crate::{
    context::Context,
    db::{DatabaseError, DatabaseStreamEntry},
    resp::Resp,
    types::{self, EntryId},
};

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
        responder: Responder<Option<Vec<String>>>,
    },
    Type {
        key: String,
        responder: Responder<String>,
    },
    Xadd {
        key: String,
        id: EntryId,
        values: HashMap<String, String>,
        wildcard: bool,
        responder: Responder<Option<String>>,
    },
    Xrange {
        key: String,
        start: Option<EntryId>,
        stop: Option<EntryId>,
        responder: Responder<Vec<DatabaseStreamEntry>>,
    },
    Xread {
        keys: Vec<String>,
        ids: Vec<EntryId>,
        block: bool,
        responder: Responder<Vec<(String, Vec<DatabaseStreamEntry>)>>,
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
    FloatParseError(#[from] std::num::ParseFloatError),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("{0}")]
    SendError(String),
    #[error("{0}")]
    DBError(#[from] DatabaseError),
    #[error("{0}")]
    EntryParseError(#[from] types::EntryIdParseErrore),
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
                    args[1].parse::<f32>()?
                } else {
                    0.0
                };

                ctx.db_sender
                    .send(RedisCommand::Blpop {
                        key: args[0].clone(),
                        responder,
                    })
                    .await?;

                let response = if count > 0.0 {
                    let timeout = timeout(Duration::from_secs_f32(count), receiver).await;
                    match timeout {
                        Ok(result) => result.unwrap(),
                        Err(_) => {
                            out.write_all(&Resp::NullBulkString.to_bytes()).await?;
                            return Ok(());
                        }
                    }
                } else {
                    receiver.await.unwrap()
                };

                if let Some(results) = response? {
                    out.write_all(&Resp::StringArray(results).to_bytes())
                        .await?;
                } else {
                    out.write_all(
                        &Resp::SimpleError("failed to get results".to_string()).to_bytes(),
                    )
                    .await?;
                }
            }
        }
        "type" => {
            if !args.len() > 1 {
                let (responder, receiver) = oneshot::channel();
                ctx.db_sender
                    .send(RedisCommand::Type {
                        key: args[0].clone(),
                        responder,
                    })
                    .await?;
                out.write_all(&Resp::SimpleString(receiver.await.unwrap()?).to_bytes())
                    .await?;
            } else {
                out.write_all(
                    &Resp::simple_error(CommandError::WrongNumArgs("list".into())).to_bytes(),
                )
                .await?;
            }
        }
        "xadd" => {
            if args.len() > 3 {
                let (responder, receiver) = oneshot::channel();
                let keys: Vec<String> = args[2..]
                    .iter()
                    .step_by(2)
                    .map(|key| key.to_string())
                    .collect();
                let values: Vec<String> = args[3..]
                    .iter()
                    .step_by(2)
                    .map(|value| value.to_string())
                    .collect();
                let mut map: HashMap<String, String> = HashMap::new();

                keys.iter().zip(values).for_each(|(key, value)| {
                    map.insert(key.to_string(), value);
                });

                let (id, wildcard) = if args[1] == "0-0" {
                    out.write_all(
                        &Resp::simple_error("The ID specified in XADD must be greater than 0-0")
                            .to_bytes(),
                    )
                    .await?;
                    return Ok(());
                } else if args[1] != "*" {
                    EntryId::new_or_wildcard_from_string(args[1].clone())?
                } else {
                    let ms_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as usize;
                    (
                        EntryId {
                            ms_time,
                            sequence: 0,
                        },
                        true,
                    )
                };

                ctx.db_sender
                    .send(RedisCommand::Xadd {
                        key: args[0].clone(),
                        id,
                        wildcard,
                        values: map,
                        responder,
                    })
                    .await?;

                let results = match receiver.await.unwrap()? {
                  Some(id) => Resp::BulkString(id).to_bytes(),
                  None => Resp::simple_error("The ID specified in XADD is equal or smaller than the target stream top item").to_bytes(),  
                };
                out.write_all(&results).await?;
            } else {
                out.write_all(
                    &Resp::simple_error(CommandError::WrongNumArgs("xadd".to_string())).to_bytes(),
                )
                .await?;
            }
        }
        "xrange" => {
            if !args.len() > 3 {
                let (responder, receiver) = oneshot::channel();
                let start = if args[1] == "-" {
                    None
                } else {
                    Some(EntryId::try_from(args[1].clone())?)
                };
                let stop = if args[2] == "+" {
                    None
                } else {
                    Some(EntryId::try_from(args[2].clone())?)
                };
                ctx.db_sender
                    .send(RedisCommand::Xrange {
                        key: args[0].clone(),
                        start,
                        stop,
                        responder,
                    })
                    .await?;
                let results = receiver.await.unwrap()?;
                if results.is_empty() {
                    out.write_all(&Resp::Array(vec![]).to_bytes()).await?;
                    return Ok(());
                }

                let output: Vec<Resp> = results.iter().map(|vals| vals.clone().into()).collect();
                out.write_all(&Resp::Array(output).to_bytes()).await?;
            } else {
                out.write_all(
                    &Resp::simple_error(CommandError::WrongNumArgs("xrange".to_string()))
                        .to_bytes(),
                )
                .await?;
            }
        }
        "xread" => {
            if args.len() > 2 {
                let (responder, receiver) = oneshot::channel();

                let (start_point, block): (usize, Option<usize>) =
                    if args[0].to_lowercase() == "block" {
                        (3, Some(args[1].parse::<usize>()?))
                    } else {
                        (1, None)
                    };

                let ids: Vec<EntryId> = args[start_point..]
                    .iter()
                    .rev()
                    .map_while(|val| EntryId::try_from(val.clone()).ok())
                    .collect();

                let keys = &args[start_point..(start_point + ids.len())];

                ctx.db_sender
                    .send(RedisCommand::Xread {
                        keys: keys.to_vec(),
                        ids,
                        block: block.is_some(),
                        responder,
                    })
                    .await?;

                let results = if let Some(time) = block {
                    match timeout(Duration::from_millis(time as u64), receiver).await {
                        Ok(out) => out.unwrap()?,
                        Err(_) => {
                            out.write_all(&Resp::NullBulkString.to_bytes()).await?;
                            return Ok(());
                        }
                    }
                } else {
                    receiver.await.unwrap()?
                };

                let output: Vec<Resp> = results
                    .iter()
                    .map(|(key, values)| {
                        Resp::Array(vec![
                            Resp::BulkString(key.to_string()),
                            Resp::Array(values.iter().map(|val| Resp::from(val.clone())).collect()),
                        ])
                    })
                    .collect();
                out.write_all(&Resp::Array(output).to_bytes()).await?;
            } else {
                out.write_all(
                    &Resp::simple_error(CommandError::WrongNumArgs("xread".to_string())).to_bytes(),
                )
                .await?;
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
