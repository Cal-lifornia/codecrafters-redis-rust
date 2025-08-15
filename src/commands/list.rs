use bytes::{Buf, Bytes};
use std::io::Read;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::{resp::Resp, types::Context};

use crate::commands::{CommandError, CommandResult};

pub async fn rpush_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    let results = ctx.db.push_list(&args[0], &args[1..]).await;
    Ok(Resp::Integer(results))
}

pub async fn lpush_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    let results = ctx.db.prepend_list(&args[0], &args[1..]).await;
    Ok(Resp::Integer(results))
}

pub async fn lrange_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if args.len() > 3 {
        return Err(CommandError::WrongNumArgs("lpush".to_string()));
    }
    let mut buf = String::new();

    args[1].clone().reader().read_to_string(&mut buf).unwrap();
    let start = buf.parse()?;

    buf.clear();

    args[2].clone().reader().read_to_string(&mut buf).unwrap();
    let end = buf.parse()?;

    let result = ctx.db.read_list(&args[0], start, end).await;

    Ok(Resp::bulk_string_array(&result))
}
pub async fn llen_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if args.len() > 2 {
        return Err(CommandError::WrongNumArgs("llen".to_string()));
    }

    let result = ctx.db.get_list_length(&args[0]).await;
    Ok(Resp::Integer(result))
}
pub async fn lpop_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if args.len() > 2 {
        return Err(CommandError::WrongNumArgs("lpop".to_string()));
    }

    let count: Option<usize> = if args.len() == 2 {
        let mut buf = String::new();
        args[1].clone().reader().read_to_string(&mut buf).unwrap();
        Some(buf.parse::<usize>()?)
    } else {
        None
    };

    let results = ctx.db.pop_front_list(&args[0], count).await;

    match results {
        Some(list) => {
            if list.len() == 1 {
                Ok(Resp::BulkString(list[0].clone()))
            } else {
                Ok(Resp::bulk_string_array(&list))
            }
        }
        None => Ok(Resp::NullBulkString),
    }
}

pub async fn blpop_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if !args.len() > 2 {
        if let Some(result) = ctx.db.check_pop_list(&args[0]).await {
            return Ok(Resp::bulk_string_array(&[args[0].clone(), result]));
        }
        let count = if args.len() == 2 {
            let mut buf = String::new();
            args[1].clone().reader().read_to_string(&mut buf).unwrap();
            buf.parse::<f32>()?
        } else {
            0.0
        };

        let (sender, receiver) = oneshot::channel();
        {
            let mut blocklist = ctx.db.list_blocklist.lock().await;
            if let Some(waiters) = blocklist.get_mut(&args[0]) {
                waiters.push(sender);
            } else {
                let _ = blocklist.insert(args[0].clone(), vec![sender]);
            }
        }

        if count > 0.0 {
            let output = timeout(Duration::from_secs_f32(count), receiver).await;
            match output {
                Ok(result) => Ok(Resp::bulk_string_array(&[args[0].clone(), result.unwrap()])),
                Err(_) => Ok(Resp::NullBulkString),
            }
        } else {
            let result = receiver.await.unwrap();
            Ok(Resp::bulk_string_array(&[args[0].clone(), result]))
        }
    } else {
        Err(CommandError::WrongNumArgs("blpop".into()))
    }
}
