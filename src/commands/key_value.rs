use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;

use crate::{resp::Resp, types::Context};

use crate::commands::CommandError;

use super::RedisCommand;

#[derive(Debug)]
pub struct SetCommandArgs {
    pub key: String,
    pub value: String,
    pub expiry: Option<Duration>,
    pub keep_ttl: bool,
}

pub async fn set_cmd(ctx: &mut Context, args: &[String]) -> Result<(), CommandError>
where
{
    let command_args = match args.len() {
        2 => SetCommandArgs {
            key: args[0].clone(),
            value: args[1].clone(),
            expiry: None,
            keep_ttl: false,
        },
        3 | 4 => {
            let (key, value, expiry_opt) = (args[0].clone(), args[1].clone(), args[2].clone());

            let keep_ttl = expiry_opt.to_lowercase().as_str() == "keepttl";
            if args.len() != 4 {
                if ctx.is_master {
                    return Err(CommandError::InvalidInput);
                } else {
                    return Ok(());
                }
            }
            let expiry_time = args[3].clone();
            let expiry = match expiry_opt.to_lowercase().as_str() {
                "ex" => Duration::from_secs(expiry_time.parse::<u64>()?),
                "px" => Duration::from_millis(expiry_time.parse::<u64>()?),
                "exat" => Duration::from_secs(expiry_time.parse::<u64>()?),
                "pxat" => Duration::from_millis(expiry_time.parse::<u64>()?),
                _ => {
                    if ctx.is_master {
                        ctx.out
                            .write()
                            .await
                            .write_all(
                                &Resp::simple_error(CommandError::InvalidCommand(
                                    expiry_opt.to_string(),
                                ))
                                .to_bytes(),
                            )
                            .await?;
                    }
                    return Ok(());
                }
            };

            SetCommandArgs {
                key,
                value,
                expiry: Some(expiry),
                keep_ttl,
            }
        }
        _ => {
            if ctx.is_master {
                ctx.out
                    .write()
                    .await
                    .write_all(
                        &Resp::simple_error(CommandError::WrongNumArgs("get".to_string()))
                            .to_bytes(),
                    )
                    .await?;
            }
            return Ok(());
        }
    };

    let (responder, receiver) = oneshot::channel();
    ctx.db_sender
        .send(RedisCommand::Set {
            args: command_args,
            responder,
        })
        .await?;
    receiver.await.unwrap()?;
    if ctx.is_master {
        ctx.out
            .write()
            .await
            .write_all(&Resp::BulkString("OK".to_string()).to_bytes())
            .await?;
    }

    Ok(())
}
pub async fn get_cmd(ctx: &mut Context, args: &[String]) -> Result<(), CommandError>
where
{
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
                ctx.out
                    .write()
                    .await
                    .write_all(&Resp::SimpleString(val).to_bytes())
                    .await?;
            }
            None => {
                ctx.out
                    .write()
                    .await
                    .write_all(&Resp::NullBulkString.to_bytes())
                    .await?;
            }
        };
    } else {
        ctx.out
            .write()
            .await
            .write_all(
                &Resp::simple_error(CommandError::WrongNumArgs("get".to_string())).to_bytes(),
            )
            .await?;
    }
    Ok(())
}

pub async fn incr_cmd(ctx: &mut Context, args: &[String]) -> Result<(), CommandError>
where
{
    let (responder, receiver) = oneshot::channel();
    ctx.db_sender
        .send(RedisCommand::Incr {
            key: args[0].clone(),
            responder,
        })
        .await?;
    ctx.out
        .write()
        .await
        .write_all(&Resp::Integer(receiver.await.unwrap()?).to_bytes())
        .await?;
    Ok(())
}
