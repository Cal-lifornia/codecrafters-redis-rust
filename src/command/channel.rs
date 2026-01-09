use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{RedisWrite, RespType},
    server::RedisError,
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
