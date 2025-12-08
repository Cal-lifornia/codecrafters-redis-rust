use bytes::{BufMut, Bytes};

use crate::resp::RespType;

impl RespType {
    pub fn simple_error(input: String) -> Self {
        Self::SimpleError(Bytes::from(input))
    }
    pub fn simple_string(input: String) -> Self {
        Self::SimpleString(Bytes::from(input))
    }
}

const CRLF: [u8; 2] = *b"\r\n";

pub trait RedisWrite {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut);
}

impl<E: std::error::Error> RedisWrite for E {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut) {
        RespType::simple_error(self.to_string()).write_to_buf(buf);
    }
}

impl RedisWrite for RespType {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut) {
        match self {
            RespType::SimpleString(bytes) => {
                buf.put_u8(b'+');
                buf.put_slice(bytes);
                buf.put_slice(&CRLF);
            }
            RespType::SimpleError(bytes) => {
                buf.put_u8(b'-');
                buf.put_slice(bytes);
                buf.put_slice(&CRLF);
            }
            RespType::Integer(num) => {
                buf.put_u8(b':');
                buf.put_slice(num.to_string().as_bytes());
                buf.put_slice(&CRLF);
            }
            RespType::BulkString(bytes) => {
                let len = bytes.len();
                buf.put_u8(b'$');
                buf.put_slice(len.to_string().as_bytes());
                buf.put_slice(&CRLF);
                buf.put_slice(bytes);
                buf.put_slice(&CRLF);
            }
            RespType::Array(resp_types) => {
                let len = resp_types.len();
                buf.put_u8(b'*');
                buf.put_slice(len.to_string().as_bytes());
                buf.put_slice(&CRLF);
                resp_types.iter().for_each(|resp| resp.write_to_buf(buf));
            }
        }
    }
}
pub struct NullBulkString;
impl RedisWrite for NullBulkString {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut) {
        buf.put_slice(b"$-1\r\n");
    }
}
