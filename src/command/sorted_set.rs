use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{NullArray, NullBulkString, RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "ZADD key score member")]
pub struct Zadd {
    key: Bytes,
    score: f64,
    member: Bytes,
}

#[async_trait]
impl AsyncCommand for Zadd {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        let idx = ctx
            .db
            .insert_sorted_set(self.key.clone(), self.member.clone(), self.score)
            .await;
        RespType::Integer(idx as i64).write_to_buf(buf);
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "ZRANK key member")]
pub struct Zrank {
    key: Bytes,
    member: Bytes,
}

#[async_trait]
impl AsyncCommand for Zrank {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        if let Some(rank) = ctx.db.get_set_member_rank(&self.key, &self.member).await {
            RespType::Integer(rank as i64).write_to_buf(buf);
        } else {
            NullBulkString.write_to_buf(buf);
        }
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "ZRANGE key start end")]
pub struct Zrange {
    key: Bytes,
    start: usize,
    end: usize,
}

#[async_trait]
impl AsyncCommand for Zrange {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        let result = ctx
            .db
            .range_sorted_set(&self.key, self.start, self.end)
            .await;
        result.write_to_buf(buf);
        Ok(())
    }
}
