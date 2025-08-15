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
        let info = ctx.info.read().await;
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
