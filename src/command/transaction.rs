use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    redis_stream::ParseStream,
    resp::{NullArray, RedisWrite, RespType},
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
        let mut transaction = ctx.transactions.write().await;
        if transaction.is_none() {
            *transaction = Some(vec![]);
            RespType::simple_string("OK").write_to_buf(buf);
        }
        Ok(())
    }
}
#[derive(RedisCommand)]
#[redis_command(syntax = "Exec", no_parse)]
pub struct Exec {}

impl ParseStream for Exec {
    fn parse_stream(
        _stream: &mut crate::redis_stream::RedisStream,
    ) -> Result<Self, crate::redis_stream::StreamParseError> {
        Ok(Self {})
    }
}

#[async_trait]
impl AsyncCommand for Exec {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let mut transactions = ctx.transactions.write().await;
        if let Some(ref cmds) = *transactions {
            if cmds.is_empty() {
                buf.put_slice(b"*0\r\n");
            } else {
                RespType::simple_string("OK").write_to_buf(buf);
                for cmd in cmds {
                    cmd.run_command(ctx, buf).await?;
                }
            }
            *transactions = None;
        } else {
            RespType::simple_error("EXEC without MULTI").write_to_buf(buf);
        }
        Ok(())
    }
}
