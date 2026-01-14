use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{NullBulkString, RedisWrite, RespType},
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
    ) -> Result<(), crate::redis::RedisError> {
        let idx = ctx
            .db
            .insert_set_member(self.key.clone(), self.member.clone(), self.score)
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
    ) -> Result<(), crate::redis::RedisError> {
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
    start: i64,
    end: i64,
}

#[async_trait]
impl AsyncCommand for Zrange {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError> {
        let result = ctx
            .db
            .range_sorted_set(&self.key, self.start, self.end)
            .await;
        result.write_to_buf(buf);
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "ZCARD key")]
pub struct Zcard {
    key: Bytes,
}

#[async_trait]
impl AsyncCommand for Zcard {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError> {
        let len = ctx.db.count_sorted_set(&self.key).await;
        RespType::Integer(len as i64).write_to_buf(buf);
        Ok(())
    }
}
#[derive(RedisCommand)]
#[redis_command(syntax = "ZSCORE key member")]
pub struct Zscore {
    key: Bytes,
    member: Bytes,
}

#[async_trait]
impl AsyncCommand for Zscore {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError> {
        if let Some(score) = ctx.db.get_set_member_score(&self.key, &self.member).await {
            RespType::BulkString(score.to_string().into()).write_to_buf(buf);
        } else {
            NullBulkString.write_to_buf(buf);
        }
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "ZREM key member")]
pub struct Zrem {
    key: Bytes,
    member: Bytes,
}

#[async_trait]
impl AsyncCommand for Zrem {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError> {
        let num = ctx.db.remove_set_member(&self.key, &self.member).await;
        RespType::Integer(num as i64).write_to_buf(buf);
        Ok(())
    }
}
