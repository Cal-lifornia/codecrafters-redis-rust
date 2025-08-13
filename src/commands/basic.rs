use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::oneshot,
};

use crate::{resp::Resp, types::Context};

use super::{CommandError, RedisCommand};

pub async fn echo_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    if args.len() == 1 {
        ctx.out
            .write_all(&Resp::BulkString(args[0].to_string()).to_bytes())
            .await?;
    } else {
        ctx.out
            .write_all(
                &Resp::SimpleError(CommandError::WrongNumArgs("echo".to_string()).to_string())
                    .to_bytes(),
            )
            .await?;
    }
    Ok(())
}

pub async fn type_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    if !args.len() > 1 {
        let (responder, receiver) = oneshot::channel();
        ctx.db_sender
            .send(RedisCommand::Type {
                key: args[0].clone(),
                responder,
            })
            .await?;
        ctx.out
            .write_all(&Resp::SimpleString(receiver.await.unwrap()?).to_bytes())
            .await?;
    } else {
        ctx.out
            .write_all(&Resp::simple_error(CommandError::WrongNumArgs("list".into())).to_bytes())
            .await?;
    }
    Ok(())
}

pub async fn info_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    if !args.is_empty() {
        let info = ctx.info();
        match args[0].to_lowercase().as_str() {
            "replication" => {
                let replication = info.replication.clone();
                ctx.out
                    .write_all(
                        &Resp::BulkString(format!(
                            "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
                            replication.role, replication.replication_id, replication.offset
                        ))
                        .to_bytes(),
                    )
                    .await?;
            }
            _ => {
                ctx.out
                    .write_all(&Resp::simple_error(CommandError::InvalidInput).to_bytes())
                    .await?;
            }
        }
    }
    Ok(())
}
