use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand, Debug)]
#[redis_command(syntax = "RPUSH key element [element ...]")]
pub struct Rpush {
    key: Bytes,
    values: Vec<Bytes>,
}

#[async_trait]
impl AsyncCommand for Rpush {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let len = ctx.db.push_list(&self.key, self.values.clone()).await;
        RespType::Integer(len).write_to_buf(buf);
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "LRANGE key start stop")]
pub struct Lrange {
    key: Bytes,
    start: i64,
    stop: i64,
}

#[async_trait]
impl AsyncCommand for Lrange {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let list = ctx.db.range_list(&self.key, self.start, self.stop).await;
        list.write_to_buf(buf);
        Ok(())
    }
}
