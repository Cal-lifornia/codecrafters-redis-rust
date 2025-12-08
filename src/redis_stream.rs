use std::rc::Rc;

use bytes::Bytes;
use hashbrown::HashMap;

use crate::{id::WildcardID, resp::RespType};

pub struct RedisStream {
    stream: Rc<Vec<Bytes>>,
    cursor: usize,
}

impl RedisStream {
    pub fn fork(&self) -> Self {
        Self {
            stream: Rc::clone(&self.stream),
            cursor: self.cursor,
        }
    }
    pub fn parse<T: ParseStream>(&mut self) -> Result<T, StreamParseError> {
        T::parse_stream(self)
    }
}

impl Iterator for RedisStream {
    type Item = Bytes;
    fn next(&mut self) -> Option<Self::Item> {
        let out = self.stream.get(self.cursor).cloned();
        self.cursor = self.stream.len().min(self.cursor + 1);
        out
    }
}

impl TryFrom<RespType> for RedisStream {
    type Error = std::io::Error;

    fn try_from(value: RespType) -> Result<Self, Self::Error> {
        if let RespType::Array(array) = value {
            let mut out: Vec<Bytes> = vec![];
            for item in array {
                if let RespType::BulkString(bstring) = item {
                    out.push(bstring);
                } else {
                    return Err(Self::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Redis commands must be an array filled with bulk strings",
                    ));
                }
            }
            Ok(RedisStream {
                stream: Rc::new(out),
                cursor: 0,
            })
        } else {
            Err(Self::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Redis commands must be submitted as an array",
            ))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StreamParseError {
    #[error("{0}")]
    IntParseError(#[from] std::num::ParseIntError),
    #[error("{0}")]
    NumParseError(#[from] std::num::ParseFloatError),
    #[error("{0}")]
    Any(#[from] Box<dyn std::error::Error>),
    #[error("Invalid number of arguments")]
    EmptyArg,
    #[error("Expected: {0}; Got: {1}")]
    Expected(&'static str, String),
}
pub trait ParseStream: Sized {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError>;
}

impl<T: ParseStream> ParseStream for Option<T> {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        let mut fork = stream.fork();
        match T::parse_stream(&mut fork) {
            Ok(out) => {
                *stream = fork;
                Ok(Some(out))
            }
            Err(_) => Ok(None),
        }
    }
}

impl ParseStream for bool {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        if let Some(value) = stream.next() {
            match value.to_ascii_lowercase().as_slice() {
                b"true" => Ok(true),
                b"false" => Ok(false),
                _ => Err(StreamParseError::Expected(
                    "true or false",
                    String::from_utf8(value.to_vec()).expect("valid utf-8"),
                )),
            }
        } else {
            Err(StreamParseError::EmptyArg)
        }
    }
}

impl ParseStream for Bytes {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        if let Some(value) = stream.next() {
            Ok(value)
        } else {
            Err(StreamParseError::EmptyArg)
        }
    }
}

// impl ParseStream for SStr {
//     fn parse_stream(stream: &mut InputStream) -> Result<Self, StreamParseError> {
//         if let Some(value) = stream.next() {
//             Ok(value)
//         } else {
//             Err(StreamParseError::EmptyArg)
//         }
//     }
// }

impl ParseStream for i64 {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        if let Some(value) = stream.next() {
            Ok(String::from_utf8(value.to_vec())
                .expect("valid utf-8")
                .parse()?)
        } else {
            Err(StreamParseError::EmptyArg)
        }
    }
}

impl ParseStream for f64 {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        if let Some(value) = stream.next() {
            Ok(String::from_utf8(value.to_vec())
                .expect("valid utf-8")
                .parse()?)
        } else {
            Err(StreamParseError::EmptyArg)
        }
    }
}
impl ParseStream for WildcardID {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        if let Some(value) = stream.next() {
            Ok(WildcardID::try_from_str(
                str::from_utf8(&value).expect("valid utf-8"),
            )?)
        } else {
            Err(StreamParseError::EmptyArg)
        }
    }
}

impl<T: ParseStream> ParseStream for Vec<T> {
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        let mut out: Vec<T> = vec![];
        while stream.peekable().peek().is_some() {
            out.push(T::parse_stream(stream)?);
        }
        Ok(out)
    }
}

impl<K, V> ParseStream for HashMap<K, V>
where
    K: ParseStream + std::hash::Hash + Eq,
    V: ParseStream,
{
    fn parse_stream(stream: &mut RedisStream) -> Result<Self, StreamParseError> {
        let mut map = HashMap::new();
        while stream.peekable().peek().is_some() {
            let key = K::parse_stream(stream)?;
            let value = V::parse_stream(stream)?;
            map.insert(key, value);
        }
        Ok(map)
    }
}
