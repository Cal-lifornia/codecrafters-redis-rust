use std::fmt::Display;

use bytes::Bytes;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RespType {
    SimpleString(Bytes),
    SimpleError(Bytes),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<RespType>),
    NullArray,
}

impl<I: Iterator<Item = S>, S: Display> From<I> for RespType {
    fn from(value: I) -> Self {
        let out: Vec<RespType> = value
            .map(|val| RespType::BulkString(Bytes::from(val.to_string())))
            .collect();
        RespType::Array(out)
    }
}
