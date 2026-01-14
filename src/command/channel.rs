use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use redis_proc_macros::RedisCommand;
use tokio::{io::AsyncWriteExt, task::JoinSet};

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
    ) -> Result<(), crate::redis::RedisError> {
        let mut channels = ctx.app_data.db.channels.write().await;
        let num = channels
            .subscribe_to_channel(self.channel.clone(), ctx.writer.clone())
            .await?;

        vec![
            RespType::bulk_string("subscribe"),
            RespType::BulkString(self.channel.clone()),
            RespType::Integer(num as i64),
        ]
        .write_to_buf(buf);
        Ok(())
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
    ) -> Result<(), crate::redis::RedisError> {
        let writers = ctx
            .app_data
            .db
            .channels
            .read()
            .await
            .get_channel_writers(&self.channel);
        tracing::debug!("WRITERS_LEN: {}", writers.len());
        RespType::Integer(writers.len() as i64).write_to_buf(buf);

        let mut msg_buf = BytesMut::new();
        vec![
            Bytes::from("message"),
            self.channel.clone(),
            self.message.clone(),
        ]
        .write_to_buf(&mut msg_buf);
        let mut task_set = JoinSet::new();
        for writer in writers {
            let writer = writer.clone();
            let msg_buf = msg_buf.clone();
            task_set.spawn(async move { writer.write().await.write_all(&msg_buf).await });
        }

        for res in task_set.join_all().await {
            if let Err(err) = res {
                tracing::warn!("failed to write to subscribed writer: {err}");
            }
        }

        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "UNSUBSCRIBE channel")]
pub struct Unsubscribe {
    channel: Bytes,
}

#[async_trait]
impl AsyncCommand for Unsubscribe {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError> {
        let num = ctx
            .app_data
            .db
            .channels
            .write()
            .await
            .unsubscribe_from_channel(&self.channel, &ctx.writer)
            .await?;

        vec![
            RespType::bulk_string("unsubscribe"),
            RespType::BulkString(self.channel.clone()),
            RespType::Integer(num as i64),
        ]
        .write_to_buf(buf);
        Ok(())
    }
}
