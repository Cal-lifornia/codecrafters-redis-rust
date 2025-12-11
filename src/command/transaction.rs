use async_trait::async_trait;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    redis_stream::ParseStream,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "MULTI", no_parse)]
pub struct Multi {}

impl ParseStream for Multi {
    fn parse_stream(
        _stream: &mut crate::redis_stream::RedisStream,
    ) -> Result<Self, crate::redis_stream::StreamParseError> {
        Ok(Self {})
    }
}

#[async_trait]
impl AsyncCommand for Multi {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let mut transaction = ctx.transaction.write().await;
        if transaction.is_none() {
            *transaction = Some(vec![]);
            RespType::simple_string("OK").write_to_buf(buf);
        }
        Ok(())
    }
}
