use std::time::Duration;

use bytes::Bytes;
use redis_proc_macros::RedisCommand;
use tokio::time::Instant;

use crate::{
    command::AsyncCommand,
    redis_stream::{ParseStream, RedisStream, StreamParseError},
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand, Debug)]
#[redis_command(
    syntax = "SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |\
    EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]"
)]
pub struct Set {
    key: Bytes,
    value: Bytes,
    expiry: Option<SetExpiryOptions>,
}

#[derive(Debug)]
pub enum SetExpiryOptions {
    // Seconds
    Ex(u64),
    // Milliseconds
    Px(u64),
    Exat(u64),
    Pxat(u64),
    KeepTTL,
}

impl ParseStream for SetExpiryOptions {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        if let Some(next) = stream.next() {
            match next.to_ascii_lowercase().as_slice() {
                b"ex" => Ok(SetExpiryOptions::Ex(stream.parse()?)),
                b"px" => Ok(SetExpiryOptions::Px(stream.parse()?)),
                b"exat" => Ok(SetExpiryOptions::Exat(stream.parse()?)),
                b"pxat" => Ok(SetExpiryOptions::Pxat(stream.parse()?)),
                b"keepttl" => Ok(SetExpiryOptions::KeepTTL),
                _ => Err(StreamParseError::Expected(
                    "expiry options".into(),
                    String::from_utf8(next.to_vec()).expect("valid utf-8"),
                )),
            }
        } else {
            Err(StreamParseError::EmptyArg)
        }
    }
}

#[async_trait::async_trait]
impl AsyncCommand for Set {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let mut expires = None::<Instant>;
        let mut keepttl = false;
        if let Some(expiry) = &self.expiry {
            match expiry {
                SetExpiryOptions::Ex(seconds) => {
                    expires = Some(Instant::now() + Duration::from_secs(*seconds));
                }
                SetExpiryOptions::Px(milliseconds) => {
                    expires = Some(Instant::now() + Duration::from_millis(*milliseconds));
                }
                SetExpiryOptions::Exat(seconds) => {
                    expires = Some(Instant::now() + Duration::from_secs(*seconds));
                }
                SetExpiryOptions::Pxat(milliseconds) => {
                    expires = Some(Instant::now() + Duration::from_millis(*milliseconds));
                }
                SetExpiryOptions::KeepTTL => {
                    keepttl = true;
                }
            }
        }
        ctx.db
            .set_string(self.key.clone(), self.value.clone(), expires, keepttl)
            .await;
        RespType::simple_string("OK".into()).write_to_buf(buf);
        Ok(())
    }
}
