use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "ACL ...")]
pub struct Acl {
    parameter: Bytes,
}

#[async_trait]
impl AsyncCommand for Acl {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        match self.parameter.to_ascii_lowercase().as_slice() {
            b"whoami" => {
                RespType::bulk_string(ctx.account.read().await.username.clone()).write_to_buf(buf);
            }
            b"getuser" => {
                let account = &ctx.account.read().await;
                RespType::Array(vec![
                    RespType::bulk_string("flags"),
                    RespType::from(account.flags.iter()),
                    RespType::bulk_string("passwords"),
                    RespType::from(account.passwords.iter()),
                ])
                .write_to_buf(buf);
            }
            _ => {
                return Err(crate::server::RedisError::Other(
                    "invalid argument".to_string(),
                ));
            }
        }
        Ok(())
    }
}
