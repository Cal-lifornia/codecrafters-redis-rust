use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::{resp::Resp, types::Context};

use crate::commands::{CommandError, CommandResult};

pub async fn exec_cmd(ctx: &Context) -> Result<(), CommandError>
where
{
    {
        let queued = ctx.queued.lock().await;
        if !(*queued) {
            return Err(CommandError::Custom("EXEC without MULTI".to_string()));
        }
    }

    handle_transactions(ctx).await?;
    Ok(())
}
pub async fn discard_cmd(ctx: &Context) -> CommandResult {
    {
        let mut queued = ctx.queued.lock().await;
        if !(*queued) {
            return Err(CommandError::Custom("DISCARD without MULTI".to_string()));
        }
        *queued = false;
    }
    {
        ctx.queue_list.lock().await.clear();
    }

    Ok(Resp::SimpleString(Bytes::from_static(b"OK")))
}

pub async fn multi_cmd(ctx: &Context) -> CommandResult {
    let mut queued = ctx.queued.lock().await;
    *queued = true;
    Ok(Resp::SimpleString(Bytes::from_static(b"OK")))
}

pub async fn handle_transactions(ctx: &Context) -> Result<(), CommandError> {
    {
        let mut queued = ctx.queued.lock().await;
        *queued = false;
    }
    let queue_list = {
        let locked_queue_list = ctx.queue_list.lock().await;
        locked_queue_list.clone()
    };
    if queue_list.is_empty() {
        ctx.out
            .write()
            .await
            .write_all(&Resp::Array(vec![]).to_bytes())
            .await?;
        return Err(CommandError::Custom("EXEC without MULTI".to_string()));
    }

    for input in queue_list {
        let result = Box::pin(input.run_command(ctx));
        result.await?
    }
    {
        let mut locked_queue_list = ctx.queue_list.lock().await;
        locked_queue_list.clear();
    }
    Ok(())
}
