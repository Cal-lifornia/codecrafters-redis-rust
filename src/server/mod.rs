use bytes::BytesMut;

use crate::{
    redis_stream::RedisStream,
    resp::{RedisWrite, RespType},
};

pub async fn handle_stream(stream: &[u8]) -> std::io::Result<bytes::Bytes> {
    let mut buf = BytesMut::new();
    let (result, _) = RespType::from_utf8(stream)?;
    let mut redis_stream = RedisStream::try_from(result)?;
    if let Some(next) = redis_stream.next() {
        match next.to_ascii_lowercase().as_slice() {
            b"ping" => RespType::simple_string("PONG".into()).write_to_buf(&mut buf),
            _ => todo!(),
        }
    } else {
        return Err(std::io::Error::other("expected a value"));
    }

    Ok(buf.into())
}
