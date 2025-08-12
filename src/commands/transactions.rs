use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::resp::{CR, LF};
use crate::{resp::Resp, types::Context};

use crate::commands::CommandError;

use super::parse_array_command;

pub async fn exec_cmd<Writer>(ctx: &mut Context<Writer>) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    {
        let queued = ctx.queued.lock().await;
        if !(*queued) {
            ctx.out
                .write_all(&Resp::simple_error("EXEC without MULTI").to_bytes())
                .await?;
            return Ok(());
        }
    }

    handle_transactions(ctx).await?;
    Ok(())
}
pub async fn discard_cmd<Writer>(ctx: &mut Context<Writer>) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    {
        let mut queued = ctx.queued.lock().await;
        if !(*queued) {
            ctx.out
                .write_all(&Resp::simple_error("DISCARD without MULTI").to_bytes())
                .await?;
            return Ok(());
        }
        *queued = false;
    }
    {
        ctx.queue_list.lock().await.clear();
    }

    ctx.out
        .write_all(&Resp::SimpleString("OK".to_string()).to_bytes())
        .await?;
    Ok(())
}

pub async fn multi_cmd<Writer>(ctx: &mut Context<Writer>) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    let mut queued = ctx.queued.lock().await;
    *queued = true;
    ctx.out
        .write_all(&Resp::SimpleString("OK".to_string()).to_bytes())
        .await?;
    Ok(())
}

pub async fn handle_transactions<Writer>(ctx: &mut Context<Writer>) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    {
        let mut queued = ctx.queued.lock().await;
        *queued = false;
    }
    let queue_list = {
        let locked_queue_list = ctx.queue_list.lock().await;
        locked_queue_list.clone()
    };
    if queue_list.is_empty() {
        ctx.out.write_all(&Resp::Array(vec![]).to_bytes()).await?;
        ctx.out
            .write_all(&Resp::simple_error("EXEC without MULTI").to_bytes())
            .await?;
        return Ok(());
    }

    ctx.out.write_all(b"*").await?;
    ctx.out
        .write_all(queue_list.len().to_string().as_bytes())
        .await?;
    ctx.out.write_all(&[CR, LF]).await?;
    for input in queue_list {
        let result = Box::pin(parse_array_command(input, ctx));
        match result.await {
            Ok(_) => {}
            Err(err) => {
                ctx.out
                    .write_all(&Resp::SimpleError(format!("ERR {err}")).to_bytes())
                    .await?;
            }
        }
    }

    {
        let mut locked_queue_list = ctx.queue_list.lock().await;
        locked_queue_list.clear();
    }
    Ok(())
}
