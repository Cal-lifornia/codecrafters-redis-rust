use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{resp::Resp, types::Context};

use super::CommandError;

pub async fn replconf_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    if args.len() == 2 {
        ctx.out
            .write_all(&Resp::SimpleString("OK".to_string()).to_bytes())
            .await?;
    } else {
        ctx.out
            .write_all(
                &Resp::simple_error(CommandError::WrongNumArgs("replconf".into())).to_bytes(),
            )
            .await?;
    }
    Ok(())
}
pub async fn psync_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    if args.len() > 2 {
        ctx.out
            .write_all(
                &Resp::str_array(&[
                    &ctx.info().replication.replication_id,
                    &ctx.info().replication.offset.to_string(),
                ])
                .to_bytes(),
            )
            .await?;
    } else {
        ctx.out
            .write_all(
                &Resp::simple_error(CommandError::WrongNumArgs("replconf".into())).to_bytes(),
            )
            .await?;
    }
    Ok(())
}
