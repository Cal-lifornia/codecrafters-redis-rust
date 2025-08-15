use bytes::{Buf, Bytes};
use std::io::Read;
use std::time::Duration;

use crate::{
    commands::{CommandError, CommandResult},
    resp::Resp,
    types::Context,
};

pub async fn set_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult
where
{
    match args.len() {
        2 => {
            ctx.db
                .set_key_value(&args[0], args[1].clone(), None, false)
                .await;
            return Ok(Resp::SimpleString(Bytes::from_static(b"OK")));
        }
        3 | 4 => {
            let mut buf = String::new();
            let (key, value, expiry_opt) = (args[0].clone(), args[1].clone(), args[2].clone());

            let keep_ttl = expiry_opt.to_ascii_lowercase().as_slice() == b"keepttl";
            if args.len() != 4 {
                return Err(CommandError::WrongNumArgs("set".into()));
            }
            args[3].clone().reader().read_to_string(&mut buf).unwrap();
            let expiry = match expiry_opt.to_ascii_lowercase().as_slice() {
                b"ex" => Duration::from_secs(buf.parse::<u64>()?),
                b"px" => Duration::from_millis(buf.parse::<u64>()?),
                b"exat" => Duration::from_secs(buf.parse::<u64>()?),
                b"pxat" => Duration::from_millis(buf.parse::<u64>()?),
                _ => {
                    buf.clear();

                    expiry_opt.reader().read_to_string(&mut buf).unwrap();
                    return Err(CommandError::InvalidArgument(buf));
                }
            };

            ctx.db
                .set_key_value(&key, value, Some(expiry), keep_ttl)
                .await;
        }
        _ => return Err(CommandError::WrongNumArgs("get".to_string())),
    }

    Ok(Resp::SimpleString(Bytes::from_static(b"OK")))
}
pub async fn get_cmd(ctx: &Context, args: &[Bytes]) -> Result<Resp, CommandError>
where
{
    if !args.len() > 1 {
        let result = match ctx.db.get_key_value(&args[0]).await {
            Some(val) => Resp::SimpleString(val),
            None => Resp::NullBulkString,
        };
        Ok(result)
    } else {
        Err(CommandError::WrongNumArgs("get".to_string()))
    }
}

pub async fn incr_cmd(ctx: &Context, args: &[Bytes]) -> Result<Resp, CommandError>
where
{
    let result = ctx.db.increase_integer(&args[0]).await?;
    Ok(Resp::Integer(result))
}
