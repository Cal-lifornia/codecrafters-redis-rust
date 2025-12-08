use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{NullBulkString, RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "GET key")]
pub struct Get {
    key: Bytes,
}

#[async_trait]
impl AsyncCommand for Get {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        match ctx.db.get_string(&self.key).await {
            Some(val) => RespType::BulkString(val).write_to_buf(buf),
            None => NullBulkString.write_to_buf(buf),
        }
        Ok(())
    }
}
