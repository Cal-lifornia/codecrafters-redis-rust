use std::collections::HashMap;

use thiserror::Error;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::oneshot,
};

mod_flat!(basic key_value stream list transactions replica);

use crate::{
    db::{DatabaseError, DatabaseStreamEntry},
    mod_flat,
    resp::Resp,
    types::{self, Context, EntryId},
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
    Incr {
        key: String,
        responder: Responder<i32>,
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
    #[error("error receiving from channel")]
    RevvError(#[from] tokio::sync::broadcast::error::RecvError),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for CommandError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::SendError(value.to_string())
    }
}

pub type Responder<T> = oneshot::Sender<Result<T, DatabaseError>>;

pub async fn parse_array_command<Writer>(
    input: Vec<Resp>,
    ctx: &mut Context<Writer>,
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    let mut inputs: Vec<String> = vec![];
    for arg in input.clone() {
        if let Resp::BulkString(val) = arg {
            inputs.push(val);
        } else {
            ctx.out
                .write_all(&Resp::SimpleString(CommandError::InvalidInput.to_string()).to_bytes())
                .await
                .unwrap();
            return Ok(());
        }
    }

    let (command, args) = inputs.split_first().unwrap();

    match command.to_lowercase().as_str() {
        "exec" => return exec_cmd(ctx).await,
        "discard" => return discard_cmd(ctx).await,
        "info" => return info_cmd(ctx, args).await,
        _ => {
            let queued = ctx.queued.lock().await;
            if *queued {
                let mut queue_list = ctx.queue_list.lock().await;
                queue_list.push(input.clone());
                ctx.out
                    .write_all(&Resp::SimpleString("QUEUED".to_string()).to_bytes())
                    .await?;
                return Ok(());
            }
        }
    }

    match command.to_lowercase().as_str() {
        "echo" => echo_cmd(ctx, args).await?,
        "ping" => {
            ctx.out
                .write_all(&Resp::SimpleString("PONG".to_string()).to_bytes())
                .await?;
        }
        "multi" => multi_cmd(ctx).await?,
        "get" => get_cmd(ctx, args).await?,
        "set" => set_cmd(ctx, args).await?,
        "incr" => incr_cmd(ctx, args).await?,
        "rpush" => rpush_cmd(ctx, args).await?,
        "lpush" => lpush_cmd(ctx, args).await?,
        "lrange" => lrange_cmd(ctx, args).await?,
        "llen" => llen_cmd(ctx, args).await?,
        "lpop" => lpop_cmd(ctx, args).await?,
        "blpop" => blpop_cmd(ctx, args).await?,
        "type" => type_cmd(ctx, args).await?,
        "xadd" => xadd_cmd(ctx, args).await?,
        "xrange" => xrange_cmd(ctx, args).await?,
        "xread" => xread_cmd(ctx, args).await?,
        "psync" => psync_cmd(ctx, args).await?,
        "replconf" => replconf_cmd(ctx, args).await?,

        _ => {
            ctx.out
                .write_all(
                    &Resp::simple_error(CommandError::InvalidCommand(inputs[0].to_string()))
                        .to_bytes(),
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
