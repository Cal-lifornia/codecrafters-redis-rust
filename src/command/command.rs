use std::fmt::Debug;

use bytes::{Bytes, BytesMut};
use either::Either;
use tokio::io::AsyncWriteExt;

use crate::{
    account::AccountError,
    command::{
        Acl, Auth, Blpop, ConfigGet, Discard, Echo, Exec, Geoadd, Geodist, Geopos, Geosearch, Get,
        Incr, Info, Keys, LLen, Lpop, Lpush, Lrange, Multi, Ping, Psync, Publish, Replconf, Rpush,
        Set, Subscribe, TypeCmd, Unsubscribe, Wait, Xadd, Xrange, Xread, Zadd, Zcard, Zrange,
        Zrank, Zrem, Zscore,
    },
    context::Context,
    redis::RedisError,
    redis_stream::{ParseStream, RedisStream},
    resp::{RedisWrite, RespType},
};

// pub type CmdAction = Box<dyn Fn(&mut Context) -> BoxFuture<Result<(), std::io::Error>>>;

pub type RedisCommand = Box<dyn Command + Send + Sync>;

pub trait Command: AsyncCommand {
    fn name(&self) -> &'static str;
    #[allow(unused)]
    fn syntax(&self) -> &'static str;
    fn is_write_cmd(&self) -> bool;
}
pub async fn handle_command(ctx: Context, input: RespType) -> Result<(), RedisError> {
    let mut redis_stream = RedisStream::try_from(input.clone())?;
    let mut buf = BytesMut::new();
    if let Some(next) = redis_stream.next() {
        let command = get_command(next, &mut redis_stream)?;
        let command_name = command.name().to_lowercase();
        let write = command.is_write_cmd();
        if let Either::Left(main) = &ctx.app_data.role
            && write
        {
            *main.need_offset.write().await = true;
            main.write_to_replicas(input.clone()).await;
        }
        if (!matches!(command_name.as_str(), "multi" | "exec" | "discard"))
            && let Some(list) = ctx.transactions.write().await.as_mut()
        {
            RespType::simple_string("QUEUED").write_to_buf(&mut buf);
            list.push(command);
        }
        // If the writer is in subscribe mode check the command that is run
        else if !matches!(
            command_name.as_str(),
            "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe" | "ping" | "quit"
        ) && ctx
            .app_data
            .db
            .channels
            .read()
            .await
            .subscribed(&ctx.writer)
            .await?
        {
            return Err(CommandError::SubscibeInvalidCommand(command.name()).into());
        } else if ctx.signed_in.read().await.is_none() {
            return Err(AccountError::NotAuthenticated.into());
        } else {
            command.run_command(&ctx, &mut buf).await?
        }

        let mut get_ack = ctx.get_ack.write().await;
        if !ctx.master_conn || *get_ack {
            *get_ack = false;
            let mut writer = ctx.writer.write().await;
            writer.write_all(&buf).await.expect("valid read");
        }
        if ctx.app_data.role.is_right() {
            let mut info = ctx.app_data.replication.write().await;
            info.offset += input.byte_size() as i64;
        }
    } else {
        return Err(RedisError::Other("expected a value".into()));
    }

    Ok(())
}

pub fn get_command(value: Bytes, stream: &mut RedisStream) -> Result<RedisCommand, RedisError> {
    match value.to_ascii_lowercase().as_slice() {
        b"ping" => Ok(Box::new(Ping {})),
        b"echo" => Ok(Box::new(Echo::parse_stream(stream)?)),
        b"type" => Ok(Box::new(TypeCmd::parse_stream(stream)?)),
        b"keys" => Ok(Box::new(Keys::parse_stream(stream)?)),
        b"set" => Ok(Box::new(Set::parse_stream(stream)?)),
        b"get" => Ok(Box::new(Get::parse_stream(stream)?)),
        b"incr" => Ok(Box::new(Incr::parse_stream(stream)?)),
        b"rpush" => Ok(Box::new(Rpush::parse_stream(stream)?)),
        b"lrange" => Ok(Box::new(Lrange::parse_stream(stream)?)),
        b"lpush" => Ok(Box::new(Lpush::parse_stream(stream)?)),
        b"llen" => Ok(Box::new(LLen::parse_stream(stream)?)),
        b"lpop" => Ok(Box::new(Lpop::parse_stream(stream)?)),
        b"blpop" => Ok(Box::new(Blpop::parse_stream(stream)?)),
        b"xadd" => Ok(Box::new(Xadd::parse_stream(stream)?)),
        b"xrange" => Ok(Box::new(Xrange::parse_stream(stream)?)),
        b"xread" => Ok(Box::new(Xread::parse_stream(stream)?)),
        b"multi" => Ok(Box::new(Multi {})),
        b"exec" => Ok(Box::new(Exec {})),
        b"discard" => Ok(Box::new(Discard {})),
        b"info" => Ok(Box::new(Info::parse_stream(stream)?)),
        b"replconf" => Ok(Box::new(Replconf::parse_stream(stream)?)),
        b"psync" => Ok(Box::new(Psync::parse_stream(stream)?)),
        b"wait" => Ok(Box::new(Wait::parse_stream(stream)?)),
        b"config" => Ok(Box::new(ConfigGet::parse_stream(stream)?)),
        b"subscribe" => Ok(Box::new(Subscribe::parse_stream(stream)?)),
        b"unsubscribe" => Ok(Box::new(Unsubscribe::parse_stream(stream)?)),
        b"publish" => Ok(Box::new(Publish::parse_stream(stream)?)),
        b"zadd" => Ok(Box::new(Zadd::parse_stream(stream)?)),
        b"zrank" => Ok(Box::new(Zrank::parse_stream(stream)?)),
        b"zrange" => Ok(Box::new(Zrange::parse_stream(stream)?)),
        b"zcard" => Ok(Box::new(Zcard::parse_stream(stream)?)),
        b"zscore" => Ok(Box::new(Zscore::parse_stream(stream)?)),
        b"zrem" => Ok(Box::new(Zrem::parse_stream(stream)?)),
        b"geoadd" => Ok(Box::new(Geoadd::parse_stream(stream)?)),
        b"geopos" => Ok(Box::new(Geopos::parse_stream(stream)?)),
        b"geodist" => Ok(Box::new(Geodist::parse_stream(stream)?)),
        b"geosearch" => Ok(Box::new(Geosearch::parse_stream(stream)?)),
        b"acl" => Ok(Box::new(Acl::parse_stream(stream)?)),
        b"auth" => Ok(Box::new(Auth::parse_stream(stream)?)),
        _ => todo!(),
    }
}

#[async_trait::async_trait]
pub trait AsyncCommand {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError>;
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("EXEC without MULTI")]
    ExecWithoutMulti,
    #[error("DISCARD without MULTI")]
    DiscardWithoutMulti,
    #[error("value is not an integer or out of range")]
    IncrInvalid,
    #[error("{0}")]
    IncorrectArgument(String),
    #[error(
        "Can't execute '{0}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
    )]
    SubscibeInvalidCommand(&'static str),
}
