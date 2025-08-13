use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::commands::replica::send_command;
use crate::{resp::Resp, types::Context};

use crate::commands::CommandError;

use super::RedisCommand;

pub async fn rpush_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    let sender = ctx.cmd_broadcaster.clone();
    let args_clone = args.to_vec();
    let replica = *ctx.tcp_replica.lock().await;
    if !replica {
        tokio::spawn(async move { send_command(sender, "rpush", &args_clone).await });
    }
    let (responder, receiver) = oneshot::channel();
    ctx.db_sender
        .send(RedisCommand::Rpush {
            key: args[0].clone(),
            values: args[1..].to_vec(),
            responder,
        })
        .await?;
    ctx.out
        .write_all(&Resp::Integer(receiver.await.unwrap()?).to_bytes())
        .await?;
    Ok(())
}

pub async fn lpush_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    let sender = ctx.cmd_broadcaster.clone();
    let args_clone = args.to_vec();
    let replica = *ctx.tcp_replica.lock().await;
    if !replica {
        tokio::spawn(async move { send_command(sender, "lpush", &args_clone).await });
    }
    let (responder, receiver) = oneshot::channel();
    ctx.db_sender
        .send(RedisCommand::Lpush {
            key: args[0].clone(),
            values: args[1..].to_vec(),
            responder,
        })
        .await?;
    ctx.out
        .write_all(&Resp::Integer(receiver.await.unwrap()?).to_bytes())
        .await?;
    Ok(())
}

pub async fn lrange_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
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
    ctx.out
        .write_all(&Resp::StringArray(receiver.await.unwrap()?).to_bytes())
        .await?;
    Ok(())
}
pub async fn llen_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
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

    ctx.out
        .write_all(&Resp::Integer(receiver.await.unwrap()?).to_bytes())
        .await?;
    Ok(())
}
pub async fn lpop_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    let sender = ctx.cmd_broadcaster.clone();
    let args_clone = args.to_vec();
    let replica = *ctx.tcp_replica.lock().await;
    if !replica {
        tokio::spawn(async move { send_command(sender, "lpop", &args_clone).await });
    }
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
                ctx.out
                    .write_all(&Resp::BulkString(list[0].clone()).to_bytes())
                    .await?
            } else {
                ctx.out
                    .write_all(&Resp::StringArray(list).to_bytes())
                    .await?
            }
        }
        None => ctx.out.write_all(&Resp::NullBulkString.to_bytes()).await?,
    };
    Ok(())
}

pub async fn blpop_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    let sender = ctx.cmd_broadcaster.clone();
    let args_clone = args.to_vec();
    let replica = *ctx.tcp_replica.lock().await;
    if !replica {
        tokio::spawn(async move { send_command(sender, "blpop", &args_clone).await });
    }
    if args.len() > 2 {
        ctx.out
            .write_all(&Resp::simple_error(CommandError::WrongNumArgs("blpop".into())).to_bytes())
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
                    ctx.out.write_all(&Resp::NullBulkString.to_bytes()).await?;
                    return Ok(());
                }
            }
        } else {
            receiver.await.unwrap()
        };

        if let Some(results) = response? {
            ctx.out
                .write_all(&Resp::StringArray(results).to_bytes())
                .await?;
        } else {
            ctx.out
                .write_all(&Resp::SimpleError("failed to get results".to_string()).to_bytes())
                .await?;
        }
    }
    Ok(())
}
