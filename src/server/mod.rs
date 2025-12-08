use bytes::{Bytes, BytesMut};

use crate::{
    command::Echo,
    redis_stream::{ParseStream, RedisStream, StreamParseError},
    resp::{RedisWrite, RespType},
};

pub async fn handle_stream(stream: &[u8]) -> Result<Bytes, RedisError> {
    let mut buf = BytesMut::new();
    let (result, _) = RespType::from_utf8(stream)?;
    let mut redis_stream = RedisStream::try_from(result)?;
    if let Some(next) = redis_stream.next() {
        match next.to_ascii_lowercase().as_slice() {
            b"ping" => RespType::simple_string("PONG".into()).write_to_buf(&mut buf),
            b"echo" => Echo::parse_stream(&mut redis_stream)?.run(&mut buf).await?,
            _ => todo!(),
        }
    } else {
        return Err(RedisError::Other("expected a value".into()));
    }

    Ok(buf.into())
}

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("{0}")]
    StreamParseError(#[from] StreamParseError),
    #[error("{0}")]
    IoError(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}
