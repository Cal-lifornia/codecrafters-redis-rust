use std::{io::Read, time::Duration};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{io::AsyncWriteExt, time::timeout};

use crate::{
    commands::CommandResult,
    resp::{self, Resp},
    types::{Context, Replica},
};

use super::CommandError;

pub async fn replconf_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if args.len() == 2 {
        if args[0].to_ascii_lowercase().as_slice() == b"getack" {
            if !ctx.ctx_info.is_master {
                let offset = ctx.app_info.read().await.replication.offset;
                let output = vec![
                    Resp::BulkString(Bytes::from_static(b"REPLCONF")),
                    Resp::BulkString(Bytes::from_static(b"ACK")),
                    Resp::BulkString(Bytes::from(offset.to_string())),
                ];
                return Ok(Resp::Array(output));
            } else {
                return Ok(getack());
            }
        }
        if args[0].to_ascii_lowercase().as_slice() == b"ack" && ctx.ctx_info.is_master {
            let current_offset = ctx.app_info.read().await.replication.replica_offset;
            let mut buf = String::new();
            args[1].clone().reader().read_to_string(&mut buf)?;
            let new_offset = buf.parse::<i32>()?;
            if new_offset > current_offset {
                ctx.app_info.write().await.replication.offset = new_offset;
                ctx.app_info.write().await.replication.replica_offset = new_offset;
            }
            {
                let waiting = *ctx.ctx_info.waiting.read().await;
                if waiting {
                    *ctx.ctx_info.returned_replicas.write().await += 1;
                }
            }
            return Err(CommandError::Ignore);
        }
        Ok(Resp::SimpleString(Bytes::from_static(b"OK")))
    } else {
        Err(CommandError::WrongNumArgs("replconf".to_string()))
    }
}

pub async fn psync_cmd(ctx: &Context, args: &[Bytes]) -> Result<(), CommandError> {
    ctx.replicas.write().await.push(Replica {
        replica: ctx.out.clone(),
    });
    if args.len() > 1 {
        let (repl_id, offset) = {
            let info = ctx.app_info.read().await;
            (
                info.replication.replication_id.clone(),
                info.replication.offset,
            )
        };
        let output = format!("FULLRESYNC {} {}", &repl_id, &offset.to_string());
        ctx.out
            .write()
            .await
            .write_all(&Resp::SimpleString(Bytes::from(output)).to_bytes())
            .await
            .unwrap();
        let binary_rdb = std::fs::read("./static/empty.rdb").unwrap();
        let mut buf = BytesMut::new();
        buf.put_u8(b'$');
        buf.put(binary_rdb.len().to_string().as_bytes());
        buf.put_slice(&[resp::CR, resp::LF]);
        buf.put_slice(&binary_rdb);
        ctx.out.write().await.write_all(&buf).await.unwrap();

        Ok(())
    } else {
        Err(CommandError::WrongNumArgs("psync".into()))
    }
}

pub async fn wait_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    let mut buf = String::new();
    args[0].clone().reader().read_to_string(&mut buf)?;
    let current_replicas = ctx.replicas.read().await.len();

    let replica_count = (buf.parse::<usize>()?).min(current_replicas);
    *ctx.ctx_info.returned_replicas.write().await = 0;

    buf.clear();
    args[1].clone().reader().read_to_string(&mut buf)?;
    let wait_time = buf.parse::<usize>()?;
    {
        *ctx.ctx_info.waiting.write().await = true;
    }
    let (main_offset, replica_offset) = {
        let info = ctx.app_info.read().await.replication.clone();
        (info.offset, info.replica_offset)
    };
    if main_offset > replica_offset {
        write_to_replicas(ctx, getack()).await?;
    } else {
        return Ok(Resp::Integer(current_replicas as i32));
    }

    if wait_time > 0 {
        match timeout(
            Duration::from_millis(wait_time as u64),
            process_wait(ctx, replica_count),
        )
        .await
        {
            Ok(output) => Ok(output?),
            Err(_) => {
                let returned_replicas = *ctx.ctx_info.returned_replicas.read().await;
                *ctx.ctx_info.waiting.write().await = false;
                *ctx.ctx_info.returned_replicas.write().await = 0;
                Ok(Resp::Integer(returned_replicas as i32))
            }
        }
    } else {
        let output = process_wait(ctx, replica_count).await?;
        *ctx.ctx_info.returned_replicas.write().await = 0;
        Ok(output)
    }
}

async fn process_wait(ctx: &Context, replica_count: usize) -> CommandResult {
    loop {
        let returned_replicas = *ctx.ctx_info.returned_replicas.read().await;
        if returned_replicas < replica_count {
            continue;
        } else {
            return Ok(Resp::Integer(returned_replicas as i32));
        }
    }
}

fn getack() -> Resp {
    let output = vec![
        Resp::BulkString(Bytes::from_static(b"REPLCONF")),
        Resp::BulkString(Bytes::from_static(b"GETACK")),
        Resp::BulkString(Bytes::from_static(b"*")),
    ];
    Resp::Array(output)
}

pub async fn write_to_replicas(ctx: &Context, input: Resp) -> Result<(), CommandError> {
    for Replica { replica } in ctx.replicas.write().await.iter_mut() {
        replica
            .write()
            .await
            .write_all(&input.clone().to_bytes())
            .await?;
    }
    Ok(())
}
