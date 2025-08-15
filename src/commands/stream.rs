use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::types::EntryId;
use crate::{resp::Resp, types::Context};

use crate::commands::{CommandError, CommandResult};

use super::RedisCommand;

pub async fn xadd_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if args.len() > 3 {
        let keys: Vec<Bytes> = args[2..].iter().step_by(2).cloned().collect();
        let values: Vec<Bytes> = args[3..].iter().step_by(2).cloned().collect();
        let mut map: HashMap<Bytes, Bytes> = HashMap::new();

        keys.iter().zip(values).for_each(|(key, value)| {
            map.insert(key.clone(), value);
        });

        let (id, wildcard) = if args[1] == "0-0" {
            return Err(CommandError::Custom(
                "The ID specified in XADD must be greater than 0-0".to_string(),
            ));
        } else if args[1] != "*" {
            EntryId::new_or_wildcard_from_bytes(args[1].clone())?
        } else {
            let ms_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as usize;
            (
                EntryId {
                    ms_time,
                    sequence: 0,
                },
                true,
            )
        };
        match ctx.db.add_stream(&args[0], id, wildcard, map).await {
            Some(id) => Ok(Resp::BulkString(Bytes::from(id))),
            None => Err(CommandError::Custom(
                "The ID specified in XADD is equal or smaller than the target stream top item"
                    .into(),
            )),
        }
    } else {
        Err(CommandError::WrongNumArgs("xadd".to_string()))
    }
}

pub async fn xrange_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if !args.len() > 3 {
        let start = if args[1] == "-" {
            None
        } else {
            Some(EntryId::try_from(args[1].clone())?)
        };
        let stop = if args[2] == "+" {
            None
        } else {
            Some(EntryId::try_from(args[2].clone())?)
        };

        let results = ctx.db.range_stream(&args[0], start, stop).await;

        if results.is_empty() {
            return Ok(Resp::Array(vec![]));
        }

        let output: Vec<Resp> = results.iter().map(|vals| vals.clone().into()).collect();
        Ok(Resp::Array(output))
    } else {
        Err(CommandError::WrongNumArgs("xrange".to_string()))
    }
}

pub async fn xread_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult
where
{
    if args.len() > 2 {
        let mut buf = String::new();
        args[1].clone().reader().read_to_string(&mut buf).unwrap();

        let (start_point, block, time): (usize, bool, usize) =
            if args[0].to_ascii_lowercase().as_slice() == b"block" {
                (3, true, buf.parse::<usize>()?)
            } else {
                (1, false, 0)
            };

        let (ids, keys): (Vec<EntryId>, Vec<Bytes>) = if args.last().unwrap() == "$" {
            (
                vec![EntryId {
                    ms_time: 0,
                    sequence: 0,
                }],
                args[start_point..args.len()].to_vec(),
            )
        } else {
            let ids: Vec<EntryId> = args[start_point..]
                .iter()
                .rev()
                .map_while(|val| EntryId::try_from(val.clone()).ok())
                .collect();

            let keys = &args[start_point..(start_point + ids.len())];
            (ids, keys.to_vec())
        };

        let (sender, receiver) = oneshot::channel();
        {
            let mut blocklist = ctx.db.stream_blocklist.lock().await;
            if let Some(waiters) = blocklist.get_mut(&args[0]) {
                waiters.push(sender);
            }
        }

        let results = if block && time != 0 {
            match timeout(Duration::from_millis(time as u64), receiver).await {
                Ok(out) => out.unwrap(),
                Err(_) => return Ok(Resp::NullBulkString),
            }
        } else {
            ctx.db.read_stream(&keys, &ids).await
        };

        let output: Vec<Resp> = results
            .iter()
            .map(|(key, values)| {
                Resp::Array(vec![
                    Resp::BulkString(key.clone()),
                    Resp::Array(values.iter().map(|val| Resp::from(val.clone())).collect()),
                ])
            })
            .collect();
        Ok(Resp::Array(output))
    } else {
        Err(CommandError::WrongNumArgs("xread".to_string()))
    }
}
