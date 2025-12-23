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
