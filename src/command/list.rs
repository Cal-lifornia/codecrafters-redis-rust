use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{NullBulkString, RedisWrite, RespType},
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

#[derive(RedisCommand)]
#[redis_command(syntax = "LPUSH key element [element ...]")]
pub struct Lpush {
    key: Bytes,
    values: Vec<Bytes>,
}

#[async_trait]
impl AsyncCommand for Lpush {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let len = ctx.db.prepend_list(&self.key, self.values.clone()).await;
        RespType::Integer(len).write_to_buf(buf);
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "LLEN key")]
pub struct LLen {
    key: Bytes,
}

#[async_trait]
impl AsyncCommand for LLen {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let len = ctx.db.list_len(&self.key).await;
        RespType::Integer(len).write_to_buf(buf);
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "LPOP key")]
pub struct Lpop {
    key: Bytes,
    count: Option<u64>,
}

#[async_trait]
impl AsyncCommand for Lpop {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let list = ctx.db.pop_list(&self.key, self.count).await;
        if !list.is_empty() {
            if self.count.is_some() {
                list.write_to_buf(buf);
            } else {
                list[0].write_to_buf(buf);
            }
        } else {
            NullBulkString.write_to_buf(buf);
        }
        Ok(())
    }
}
