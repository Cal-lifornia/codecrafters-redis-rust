use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

use crate::resp::RespType;

pub struct RedisRespCodec;

impl Decoder for RedisRespCodec {
    type Item = RespType;
    type Error = std::io::Error;
    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let first = src.first().cloned();
        src.advance(1);
        match first {
            Some(b'+') => {
                let breakoff = match src.windows(2).position(|window| window == b"\r\n") {
                    Some(idx) => idx,
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "missing '\\r\\n'",
                        ));
                    }
                };
                let out = Bytes::copy_from_slice(&src[0..breakoff]);
                src.advance(breakoff + 2);
                Ok(Some(RespType::SimpleString(out)))
            }
            Some(b'-') => {
                let breakoff = match src.windows(2).position(|window| window == b"\r\n") {
                    Some(idx) => idx,
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "missing '\\r\\n'",
                        ));
                    }
                };
                let out = Bytes::copy_from_slice(&src[0..breakoff]);
                src.advance(breakoff + 2);
                Ok(Some(RespType::SimpleError(out)))
            }
            Some(b':') => {
                let breakoff = match src.windows(2).position(|window| window == b"\r\n") {
                    Some(idx) => idx,
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "missing '\\r\\n'",
                        ));
                    }
                };
                let num = String::from_utf8_lossy(&src[0..breakoff])
                    .parse::<i64>()
                    .map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("expected valid integer: {err}"),
                        )
                    })?;
                src.advance(breakoff + 2);
                Ok(Some(RespType::Integer(num)))
            }
            Some(b'$') => {
                let size_breakoff = match src.windows(2).position(|window| window == b"\r\n") {
                    Some(idx) => idx,
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "missing '\\r\\n'",
                        ));
                    }
                };
                src.advance(size_breakoff + 2);
                let size = String::from_utf8_lossy(&src[0..size_breakoff])
                    .parse::<i64>()
                    .map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("expected valid integer: {err}"),
                        )
                    })?;
                if size == -1 {
                    Ok(Some(RespType::NullBulkString))
                } else if size >= 0 {
                    let out = Bytes::copy_from_slice(&src[0..(size as usize)]);
                    src.advance((size as usize) + 2);
                    Ok(Some(RespType::BulkString(out)))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid bulk string length",
                    ))
                }
            }
            Some(b'*') => {
                let size_breakoff = match src.windows(2).position(|window| window == b"\r\n") {
                    Some(idx) => idx,
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "missing '\\r\\n'",
                        ));
                    }
                };
                let size = String::from_utf8_lossy(&src[0..size_breakoff])
                    .parse::<i64>()
                    .map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("expected valid integer: {err}"),
                        )
                    })?;
                src.advance(size_breakoff + 2);
                if size == -1 {
                    Ok(Some(RespType::NullArray))
                } else if size >= 0 {
                    let mut out = vec![];
                    for _ in 0..size {
                        if let Some(item) = self.decode(src)? {
                            out.push(item);
                        } else {
                            return Ok(None);
                        }
                    }
                    Ok(Some(RespType::Array(out)))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid bulk string length",
                    ))
                }
            }
            Some(ch) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("'{ch}' is an invalid first byte"),
            )),
            None => Ok(None),
        }
    }
}
