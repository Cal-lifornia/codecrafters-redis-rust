use std::hash::Hash;

use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{NullBulkString, RedisWrite, RespType},
};

#[derive(Debug, Clone)]
pub struct SortedSetValue {
    member: Bytes,
    score: f64,
}

impl Hash for SortedSetValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.member.hash(state);
    }
}

impl PartialEq for SortedSetValue {
    fn eq(&self, other: &Self) -> bool {
        self.member == other.member && self.score == other.score
    }
}

impl PartialOrd for SortedSetValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score
            .partial_cmp(&other.score)
            .map(|ord| ord.then(self.member.cmp(&other.member)))
    }
}

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
        if let Some(rank) = ctx.db.get_rank(&self.key, &self.member).await {
            RespType::Integer(rank as i64).write_to_buf(buf);
        } else {
            NullBulkString.write_to_buf(buf);
        }
        Ok(())
    }
}
