use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::redis_stream::{ParseStream, RedisStream, StreamParseError};

#[derive(RedisCommand)]
#[redis_command(
    syntax = "SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |\
    EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]"
)]
pub struct Set {
    key: Bytes,
    value: Bytes,
    expiry: Option<SetExpiryOptions>,
}

pub enum SetExpiryOptions {
    Ex(Bytes),
    Px(Bytes),
    Exat(Bytes),
    Pxat(Bytes),
    KeepTTL,
}

impl ParseStream for SetExpiryOptions {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        if let Some(next) = stream.next() {
            match next.to_ascii_lowercase().as_slice() {
                b"ex" => Ok(SetExpiryOptions::Ex(stream.parse()?)),
                b"bx" => Ok(SetExpiryOptions::Px(stream.parse()?)),
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
