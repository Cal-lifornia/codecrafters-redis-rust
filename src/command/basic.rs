use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "ECHO message")]
pub struct Echo {
    message: Bytes,
}

#[async_trait]
impl AsyncCommand for Echo {
    async fn run_command(
        &self,
        _ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        self.message.write_to_buf(buf);
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "TYPE key")]
pub struct TypeCmd {
    key: Bytes,
}

#[async_trait]
impl AsyncCommand for TypeCmd {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let value = ctx.db.db_type(&self.key).await;
        RespType::SimpleString(value).write_to_buf(buf);
        Ok(())
    }
}
