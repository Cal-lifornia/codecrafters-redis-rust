use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "SUBSCRIBE channel")]
pub struct Subscribe {
    channel: Bytes,
}

#[async_trait]
impl AsyncCommand for Subscribe {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        ctx.db
            .subscibe_to_channel(self.channel.clone(), ctx.writer.clone())
            .await;
        vec![
            RespType::bulk_string("subscribe"),
            RespType::BulkString(self.channel.clone()),
            RespType::Integer(1),
        ]
        .write_to_buf(buf);
        Ok(())
    }
}
