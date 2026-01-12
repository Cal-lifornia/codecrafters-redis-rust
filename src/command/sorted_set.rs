use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{RedisWrite, RespType},
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
