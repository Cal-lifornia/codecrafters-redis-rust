use std::io::stdout;

use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::{resp::Resp, types::Context};

use crate::commands::{CommandError, CommandResult};

pub async fn exec_cmd(ctx: &Context) -> CommandResult
where
{
    {
        let queued = ctx.queued.lock().await;
        if !(*queued) {
            return Err(CommandError::Custom("EXEC without MULTI".to_string()));
        }
    }

    let results = handle_transactions(ctx).await?;
    Ok(results)
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

pub async fn handle_transactions(ctx: &Context) -> CommandResult {
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
    let mut output = vec![];
    for input in queue_list {
        let result = Box::pin(input.run_command_single(ctx));
        let value = match result.await {
            Ok(out) => out,
            Err(err) => Resp::SimpleError(Bytes::from(format!("ERR {err}"))),
        };
        output.push(value);
    }
    {
        let mut locked_queue_list = ctx.queue_list.lock().await;
        locked_queue_list.clear();
    }
    Ok(Resp::Array(output))
}
