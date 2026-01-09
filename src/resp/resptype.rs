use std::fmt::Display;

use bytes::Bytes;

use crate::rdb::EncodedRdbFile;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RespType {
    SimpleString(Bytes),
    SimpleError(Bytes),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<RespType>),
    NullArray,
    RdbFile(EncodedRdbFile),
}

impl RespType {
    pub fn bulk_string<S: Display>(value: S) -> RespType {
        RespType::BulkString(Bytes::from(value.to_string()))
    }
}

impl<I: Iterator<Item = S>, S: Display> From<I> for RespType {
    fn from(value: I) -> Self {
        let out: Vec<RespType> = value
            .map(|val| RespType::BulkString(Bytes::from(val.to_string())))
            .collect();
        RespType::Array(out)
    }
}

impl RespType {
    pub fn byte_size(&self) -> usize {
        match self {
            RespType::SimpleString(value) => value.len() + 3,
            RespType::SimpleError(value) => value.len() + 3,
            RespType::Integer(num) => num.to_string().len() + 3,
            RespType::BulkString(value) => {
                let len = value.len();
                let len_size = len.to_string().len() + 3;
                len + len_size + 2
            }
            RespType::NullBulkString => 5,
            RespType::Array(values) => {
                let len_size = values.len().to_string().len() + 3;
                values.iter().map(|val| val.byte_size()).sum::<usize>() + len_size
            }
            RespType::NullArray => 5,
            RespType::RdbFile(file) => file.0.len() + file.0.len().to_string().len() + 3,
        }
    }
}
