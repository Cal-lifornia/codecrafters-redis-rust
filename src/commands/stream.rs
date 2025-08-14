use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::types::EntryId;
use crate::{resp::Resp, types::Context};

use crate::commands::CommandError;

use super::RedisCommand;

pub async fn xadd_cmd(ctx: &mut Context, args: &[String]) -> Result<(), CommandError>
where
{
    if args.len() > 3 {
        let (responder, receiver) = oneshot::channel();
        let keys: Vec<String> = args[2..]
            .iter()
            .step_by(2)
            .map(|key| key.to_string())
            .collect();
        let values: Vec<String> = args[3..]
            .iter()
            .step_by(2)
            .map(|value| value.to_string())
            .collect();
        let mut map: HashMap<String, String> = HashMap::new();

        keys.iter().zip(values).for_each(|(key, value)| {
            map.insert(key.to_string(), value);
        });

        let (id, wildcard) = if args[1] == "0-0" {
            ctx.out
                .write()
                .await
                .write_all(
                    &Resp::simple_error("The ID specified in XADD must be greater than 0-0")
                        .to_bytes(),
                )
                .await?;
            return Ok(());
        } else if args[1] != "*" {
            EntryId::new_or_wildcard_from_string(args[1].clone())?
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

        ctx.db_sender
            .send(RedisCommand::Xadd {
                key: args[0].clone(),
                id,
                wildcard,
                values: map,
                responder,
            })
            .await?;

        let results = match receiver.await.unwrap()? {
            Some(id) => Resp::BulkString(id).to_bytes(),
            None => Resp::simple_error(
                "The ID specified in XADD is equal or smaller than the target stream top item",
            )
            .to_bytes(),
        };
        ctx.out.write().await.write_all(&results).await?;
    } else {
        ctx.out
            .write()
            .await
            .write_all(
                &Resp::simple_error(CommandError::WrongNumArgs("xadd".to_string())).to_bytes(),
            )
            .await?;
    }
    Ok(())
}

pub async fn xrange_cmd(ctx: &mut Context, args: &[String]) -> Result<(), CommandError>
where
{
    if !args.len() > 3 {
        let (responder, receiver) = oneshot::channel();
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
        ctx.db_sender
            .send(RedisCommand::Xrange {
                key: args[0].clone(),
                start,
                stop,
                responder,
            })
            .await?;
        let results = receiver.await.unwrap()?;
        if results.is_empty() {
            ctx.out
                .write()
                .await
                .write_all(&Resp::Array(vec![]).to_bytes())
                .await?;
            return Ok(());
        }

        let output: Vec<Resp> = results.iter().map(|vals| vals.clone().into()).collect();
        ctx.out
            .write()
            .await
            .write_all(&Resp::Array(output).to_bytes())
            .await?;
    } else {
        ctx.out
            .write()
            .await
            .write_all(
                &Resp::simple_error(CommandError::WrongNumArgs("xrange".to_string())).to_bytes(),
            )
            .await?;
    }
    Ok(())
}

pub async fn xread_cmd(ctx: &mut Context, args: &[String]) -> Result<(), CommandError>
where
{
    if args.len() > 2 {
        let (responder, receiver) = oneshot::channel();

        let (start_point, block, time): (usize, bool, usize) = if args[0].to_lowercase() == "block"
        {
            (3, true, args[1].parse::<usize>()?)
        } else {
            (1, false, 0)
        };

        let (ids, keys): (Vec<EntryId>, Vec<String>) = if args.last().unwrap() == "$" {
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

        ctx.db_sender
            .send(RedisCommand::Xread {
                keys,
                ids,
                block,
                responder,
            })
            .await?;

        let results = if block && time != 0 {
            match timeout(Duration::from_millis(time as u64), receiver).await {
                Ok(out) => out.unwrap()?,
                Err(_) => {
                    ctx.out
                        .write()
                        .await
                        .write_all(&Resp::NullBulkString.to_bytes())
                        .await?;
                    return Ok(());
                }
            }
        } else {
            receiver.await.unwrap()?
        };

        let output: Vec<Resp> = results
            .iter()
            .map(|(key, values)| {
                Resp::Array(vec![
                    Resp::BulkString(key.to_string()),
                    Resp::Array(values.iter().map(|val| Resp::from(val.clone())).collect()),
                ])
            })
            .collect();
        ctx.out
            .write()
            .await
            .write_all(&Resp::Array(output).to_bytes())
            .await?;
    } else {
        ctx.out
            .write()
            .await
            .write_all(
                &Resp::simple_error(CommandError::WrongNumArgs("xread".to_string())).to_bytes(),
            )
            .await?;
    }
    Ok(())
}
