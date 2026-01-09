use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;
use tokio::task::JoinSet;

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
        let mut channels = ctx.db.channels.write().await;
        match channels
            .subscribe_to_channel(self.channel.clone(), ctx.writer.clone())
            .await
        {
            Ok(num) => {
                vec![
                    RespType::bulk_string("subscribe"),
                    RespType::BulkString(self.channel.clone()),
                    RespType::Integer(num as i64),
                ]
                .write_to_buf(buf);
                Ok(())
            }
            Err(err) => {
                if let crate::server::RedisError::Other(err_msg) = err {
                    RespType::simple_error(err_msg).write_to_buf(buf);
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "PUBLISH channel message")]
pub struct Publish {
    channel: Bytes,
    message: Bytes,
}

#[async_trait]
impl AsyncCommand for Publish {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        let writers = ctx
            .db
            .channels
            .read()
            .await
            .get_channel_writers(&self.channel)
            .await;
        RespType::Integer(writers.len() as i64).write_to_buf(buf);
        Ok(())
    }
}
