use bytes::Bytes;

use crate::{commands::CommandResult, resp::Resp, types::Context};

use super::CommandError;

pub async fn echo_cmd(args: &[Bytes]) -> CommandResult {
    if args.len() == 1 {
        Ok(Resp::BulkString(args[0].clone()))
    } else {
        Err(CommandError::WrongNumArgs("echo".to_string()))
    }
}

pub async fn type_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if !args.is_empty() {
        let result = ctx.db.get_type(&args[0]).await;
        Ok(Resp::BulkString(Bytes::from(result)))
    } else {
        Err(CommandError::WrongNumArgs("list".into()))
    }
}

pub async fn info_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if !args.is_empty() {
        let info = ctx.app_info.read().await;
        match args[0].to_ascii_lowercase().as_slice() {
            b"replication" => {
                let replication = info.replication.clone();
                let output = format!(
                    "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
                    replication.role, replication.replication_id, replication.offset
                );
                Ok(Resp::BulkString(Bytes::from(output)))
            }
            _ => Err(CommandError::InvalidInput),
        }
    } else {
        Err(CommandError::WrongNumArgs("info".to_string()))
    }
}

pub async fn config_cmd(ctx: &Context, args: &[Bytes]) -> CommandResult {
    if !args.is_empty() {
        match args[0].to_ascii_lowercase().as_slice() {
            b"get" => match args[1].to_ascii_lowercase().as_slice() {
                b"dir" => Ok(Resp::bulk_string_array(&[
                    args[1].clone(),
                    Bytes::from(ctx.app_info.read().await.config.dir.clone()),
                ])),
                b"dbfilename" => Ok(Resp::bulk_string_array(&[
                    args[1].clone(),
                    Bytes::from(ctx.app_info.read().await.config.db_filename.clone()),
                ])),
                _ => Err(CommandError::InvalidInput),
            },
            _ => Err(CommandError::InvalidInput),
        }
    } else {
        Err(CommandError::WrongNumArgs("args".to_string()))
    }
}
