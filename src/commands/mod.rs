use std::sync::Arc;

use tokio::{io::AsyncWriteExt, sync::Mutex};

use crate::{mod_flat, resp::Resp};

use bytes::Bytes;
use thiserror::Error;

use crate::types::{self, Context};
mod_flat!(basic key_value stream list transactions replica);

pub type CommandQueueList = Arc<Mutex<Vec<RedisCommand>>>;

pub(crate) type CommandResult = Result<Resp, CommandError>;

#[derive(Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(Vec<Bytes>),
    Get(Vec<Bytes>),
    Set(Vec<Bytes>),
    Incr(Vec<Bytes>),
    Rpush(Vec<Bytes>),
    Lpush(Vec<Bytes>),
    Lrange(Vec<Bytes>),
    LLen(Vec<Bytes>),
    Lpop(Vec<Bytes>),
    Blpop(Vec<Bytes>),
    Type(Vec<Bytes>),
    Xadd(Vec<Bytes>),
    Xrange(Vec<Bytes>),
    Xread(Vec<Bytes>),
    Exec,
    Discard,
    Info(Vec<Bytes>),
    Multi,
    Psync(Vec<Bytes>),
    Replconf(Vec<Bytes>),
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("invalid input")]
    InvalidInput,
    #[error("invalid number of arguments for {0}")]
    WrongNumArgs(String),
    #[error("error parsing input {0}")]
    IntParseError(#[from] std::num::ParseIntError),
    #[error("invalid argument {0}")]
    InvalidArgument(String),
    #[error("error parsing input {0}")]
    FloatParseError(#[from] std::num::ParseFloatError),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("{0}")]
    EntryParseError(#[from] types::EntryIdParseError),
    #[error("{0}")]
    Custom(String),
}

impl RedisCommand {
    pub fn is_write_command(&self) -> bool {
        use RedisCommand::*;
        match self {
            Ping => false,
            Echo(_) => false,
            Get(_) => false,
            Set(_) => true,
            Incr(_) => true,
            Rpush(_) => true,
            Lpush(_) => true,
            Lrange(_) => false,
            LLen(_) => false,
            Lpop(_) => true,
            Blpop(_) => true,
            Type(_) => false,
            Xadd(_) => true,
            Xrange(_) => false,
            Xread(_) => false,
            Exec => false,
            Discard => false,
            Info(_) => false,
            Multi => false,
            Psync(_) => false,
            Replconf(_) => false,
        }
    }

    pub async fn run_command_full(self, ctx: &Context) -> Result<(), CommandError> {
        use RedisCommand::*;
        match self {
            Exec => {
                match exec_cmd(ctx).await {
                    Ok(results) => {
                        ctx.out
                            .write()
                            .await
                            .write_all(&results.to_bytes())
                            .await
                            .unwrap();
                    }
                    Err(err) => {
                        ctx.out
                            .write()
                            .await
                            .write_all(&Resp::simple_error(format!("ERR {err}")).to_bytes())
                            .await
                            .unwrap();
                    }
                }
                return Ok(());
            }
            Discard => match discard_cmd(ctx).await {
                Ok(output) => {
                    ctx.out.write().await.write_all(&output.to_bytes()).await?;
                    return Ok(());
                }
                Err(err) => {
                    ctx.out
                        .write()
                        .await
                        .write_all(&Resp::simple_error(format!("ERR {err}")).to_bytes())
                        .await
                        .unwrap();
                    return Ok(());
                }
            },
            Info(ref items) => {
                let output = info_cmd(ctx, items).await?;
                ctx.out.write().await.write_all(&output.to_bytes()).await?;
                return Ok(());
            }
            Psync(ref items) => {
                psync_cmd(ctx, items).await?;
            }
            _ => {
                if ctx.ctx_info.is_master {
                    let queued = ctx.queued.lock().await;
                    if *queued {
                        let mut queue_list = ctx.queue_list.lock().await;
                        queue_list.push(self);
                        ctx.out
                            .write()
                            .await
                            .write_all(
                                &Resp::SimpleString(Bytes::from_static(b"QUEUED")).to_bytes(),
                            )
                            .await?;
                        return Ok(());
                    }
                }
            }
        }

        let result = match self {
            Ping => Ok(Resp::SimpleString(Bytes::from_static(b"PONG"))),
            Echo(ref items) => echo_cmd(items).await,
            Get(ref items) => get_cmd(ctx, items).await,
            Set(ref items) => set_cmd(ctx, items).await,
            Incr(ref items) => incr_cmd(ctx, items).await,
            Rpush(ref items) => rpush_cmd(ctx, items).await,
            Lpush(ref items) => lpush_cmd(ctx, items).await,
            Lrange(ref items) => lrange_cmd(ctx, items).await,
            LLen(ref items) => llen_cmd(ctx, items).await,
            Lpop(ref items) => lpop_cmd(ctx, items).await,
            Blpop(ref items) => blpop_cmd(ctx, items).await,
            Type(ref items) => type_cmd(ctx, items).await,
            Xadd(ref items) => xadd_cmd(ctx, items).await,
            Xrange(ref items) => xrange_cmd(ctx, items).await,
            Xread(ref items) => xread_cmd(ctx, items).await,
            Replconf(ref items) => replconf_cmd(ctx, items).await,
            Multi => multi_cmd(ctx).await,
            _ => unreachable!(),
        };
        if ctx.ctx_info.is_master && self.is_write_command() {
            write_to_replicas(ctx, self.clone().into()).await?;
        }
        if !ctx.ctx_info.is_master && ctx.ctx_info.stream_from_master {
            let offset = Resp::from(self.clone()).to_bytes().len() as i32;
            ctx.app_info.write().await.replication.offset += offset;
        }

        match result {
            Ok(output) => {
                if (ctx.ctx_info.is_master && self.is_write_command()) || !self.is_write_command() {
                    ctx.out
                        .write()
                        .await
                        .write_all(&output.to_bytes())
                        .await
                        .unwrap();
                }
            }
            Err(err) => {
                if ctx.ctx_info.is_master {
                    ctx.out
                        .write()
                        .await
                        .write_all(&Resp::simple_error(format!("ERR {err}")).to_bytes())
                        .await
                        .unwrap();
                }
            }
        };
        Ok(())
    }

    pub async fn run_command_single(self, ctx: &Context) -> Result<Resp, CommandError> {
        use RedisCommand::*;
        match self {
            Ping => Ok(Resp::SimpleString(Bytes::from_static(b"PONG"))),
            Echo(ref items) => echo_cmd(items).await,
            Get(ref items) => get_cmd(ctx, items).await,
            Set(ref items) => set_cmd(ctx, items).await,
            Incr(ref items) => incr_cmd(ctx, items).await,
            Rpush(ref items) => rpush_cmd(ctx, items).await,
            Lpush(ref items) => lpush_cmd(ctx, items).await,
            Lrange(ref items) => lrange_cmd(ctx, items).await,
            LLen(ref items) => llen_cmd(ctx, items).await,
            Lpop(ref items) => lpop_cmd(ctx, items).await,
            Blpop(ref items) => blpop_cmd(ctx, items).await,
            Type(ref items) => type_cmd(ctx, items).await,
            Xadd(ref items) => xadd_cmd(ctx, items).await,
            Xrange(ref items) => xrange_cmd(ctx, items).await,
            Xread(ref items) => xread_cmd(ctx, items).await,
            Multi => multi_cmd(ctx).await,
            _ => unreachable!(),
        }
    }
}

impl TryFrom<Vec<Bytes>> for RedisCommand {
    type Error = std::io::Error;

    fn try_from(value: Vec<Bytes>) -> Result<Self, Self::Error> {
        use RedisCommand::*;

        match value[0].to_ascii_lowercase().as_slice() {
            b"ping" => Ok(Ping),
            b"echo" => Ok(Echo(value[1..].to_vec())),
            b"get" => Ok(Get(value[1..].to_vec())),
            b"set" => Ok(Set(value[1..].to_vec())),
            b"incr" => Ok(Incr(value[1..].to_vec())),
            b"rpush" => Ok(Rpush(value[1..].to_vec())),
            b"lpush" => Ok(Lpush(value[1..].to_vec())),
            b"llen" => Ok(LLen(value[1..].to_vec())),
            b"lrange" => Ok(Lrange(value[1..].to_vec())),
            b"lpop" => Ok(Lpop(value[1..].to_vec())),
            b"blpop" => Ok(Blpop(value[1..].to_vec())),
            b"type" => Ok(Type(value[1..].to_vec())),
            b"xadd" => Ok(Xadd(value[1..].to_vec())),
            b"xrange" => Ok(Xrange(value[1..].to_vec())),
            b"xread" => Ok(Xread(value[1..].to_vec())),
            b"exec" => Ok(Exec),
            b"discard" => Ok(Discard),
            b"info" => Ok(Info(value[1..].to_vec())),
            b"multi" => Ok(Multi),
            b"psync" => Ok(Psync(value[1..].to_vec())),
            b"replconf" => Ok(Replconf(value[1..].to_vec())),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "command does not exist",
            )),
        }
    }
}
impl TryFrom<Vec<Resp>> for RedisCommand {
    type Error = std::io::Error;

    fn try_from(value: Vec<Resp>) -> Result<Self, Self::Error> {
        let mut inputs = vec![];
        for input in value {
            if let Resp::BulkString(val) = input {
                inputs.push(val);
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "not a redis bulk string",
                ));
            }
        }
        RedisCommand::try_from(inputs)
    }
}

impl From<RedisCommand> for Resp {
    fn from(value: RedisCommand) -> Self {
        use RedisCommand::*;
        let mut output: Vec<Bytes> = vec![];
        match value {
            Ping => {
                output.push(Bytes::from_static(b"into"));
            }
            Echo(items) => {
                output.push(Bytes::from_static(b"echo"));
                output.extend_from_slice(&items);
            }
            Get(items) => {
                output.push(Bytes::from_static(b"get"));
                output.extend_from_slice(&items);
            }
            Set(items) => {
                output.push(Bytes::from_static(b"set"));
                output.extend_from_slice(&items);
            }
            Incr(items) => {
                output.push(Bytes::from_static(b"incr"));
                output.extend_from_slice(&items);
            }
            Rpush(items) => {
                output.push(Bytes::from_static(b"rpush"));
                output.extend_from_slice(&items);
            }
            Lpush(items) => {
                output.push(Bytes::from_static(b"lpush"));
                output.extend_from_slice(&items);
            }
            Lrange(items) => {
                output.push(Bytes::from_static(b"lrange"));
                output.extend_from_slice(&items);
            }
            LLen(items) => {
                output.push(Bytes::from_static(b"llen"));
                output.extend_from_slice(&items);
            }
            Lpop(items) => {
                output.push(Bytes::from_static(b"lpop"));
                output.extend_from_slice(&items);
            }
            Blpop(items) => {
                output.push(Bytes::from_static(b"blpop"));
                output.extend_from_slice(&items);
            }
            Type(items) => {
                output.push(Bytes::from_static(b"type"));
                output.extend_from_slice(&items);
            }
            Xadd(items) => {
                output.push(Bytes::from_static(b"xadd"));
                output.extend_from_slice(&items);
            }
            Xrange(items) => {
                output.push(Bytes::from_static(b"xrange"));
                output.extend_from_slice(&items);
            }
            Xread(items) => {
                output.push(Bytes::from_static(b"xread"));
                output.extend_from_slice(&items);
            }
            Exec => {
                output.push(Bytes::from_static(b"exec"));
            }
            Discard => {
                output.push(Bytes::from_static(b"discard"));
            }
            Info(items) => {
                output.push(Bytes::from_static(b"info"));
                output.extend_from_slice(&items);
            }
            Multi => {
                output.push(Bytes::from_static(b"multi"));
            }
            Psync(items) => {
                output.push(Bytes::from_static(b"psync"));
                output.extend_from_slice(&items);
            }
            Replconf(items) => {
                output.push(Bytes::from_static(b"replconf"));
                output.extend_from_slice(&items);
            }
        }
        Resp::bulk_string_array(&output)
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
