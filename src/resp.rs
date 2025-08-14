use std::{collections::HashMap, fmt::Display};

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::db::DatabaseStreamEntry;

pub type RespResult<'a> = std::result::Result<(Resp, &'a [u8]), RespError>;

#[derive(Debug, Error)]
pub enum RespError {
    #[error("not enough bytes")]
    NotEnoughBytes,
    #[error("incorrect format")]
    IncorrectFormat,
    #[error("error parsing input {0}")]
    IntParseError(#[from] std::num::ParseIntError),
    #[error("error parsing input {0}")]
    FloatParseError(#[from] std::num::ParseFloatError),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
}

pub const CR: u8 = b'\r';
pub const LF: u8 = b'\n';

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Resp {
    SimpleString(String),
    SimpleError(String),
    Integer(i32),
    BulkString(String),
    Array(Vec<Resp>),
    StringArray(Vec<String>),
    NullBulkString,
}

impl Resp {
    pub fn to_bytes(self) -> Bytes {
        self.into()
    }
    pub fn simple_error(input: impl Display) -> Self {
        Self::SimpleError(format!("ERR {input}"))
    }
    pub fn str_array(input: &[&str]) -> Self {
        Resp::Array(
            input
                .iter()
                .map(|value| Resp::BulkString(value.to_string()))
                .collect(),
        )
    }
}

// pub fn byte_vec_to_resp(inputs: Vec<&[u8]>) -> Bytes {
//     let mut result = BytesMut::new();
//     result.put_u8(b'*');
//     result.put(inputs.len().to_string().as_bytes());
//     for input in inputs {
//         result.put(input);
//     }
//     result.into()
// }

impl From<Resp> for Bytes {
    fn from(val: Resp) -> Self {
        use Resp::*;
        let mut buf = BytesMut::new();
        match val {
            SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put(s.as_bytes());
                buf.put_slice(&[CR, LF]);
            }
            SimpleError(s) => {
                buf.put_u8(b'-');
                buf.put(s.as_bytes());
                buf.put_slice(&[CR, LF]);
                eprintln!("ERR {s}");
            }
            Integer(i) => {
                buf.put_u8(b':');
                buf.put(format!("{i}").as_bytes());
                buf.put_slice(&[CR, LF]);
            }
            BulkString(s) => {
                buf.put_u8(b'$');
                buf.put(s.len().to_string().as_bytes());
                buf.put_slice(&[CR, LF]);
                buf.put(s.as_bytes());
                buf.put_slice(&[CR, LF]);
            }
            NullBulkString => {
                buf.put(&b"$-1"[..]);
                buf.put_slice(&[CR, LF]);
            }
            Array(v) => {
                buf.put_u8(b'*');
                buf.put(v.len().to_string().as_bytes());
                buf.put_slice(&[CR, LF]);
                for resp in v {
                    let array_buf: Bytes = resp.into();
                    buf.put(array_buf);
                }
            }
            StringArray(s) => {
                buf.put_u8(b'*');
                buf.put(s.len().to_string().as_bytes());
                buf.put_slice(&[CR, LF]);
                for item in s {
                    let string_buf: Bytes = BulkString(item.to_string()).into();
                    buf.put(string_buf);
                }
            }
        }
        buf.into()
    }
}

impl From<HashMap<String, String>> for Resp {
    fn from(value: HashMap<String, String>) -> Self {
        let mut results: Vec<String> = vec![];
        value.iter().for_each(|(key, val)| {
            results.push(key.to_string());
            results.push(val.to_string());
        });
        Resp::StringArray(results)
    }
}

impl From<DatabaseStreamEntry> for Resp {
    fn from(value: DatabaseStreamEntry) -> Self {
        Resp::Array(vec![Resp::BulkString(value.id.into()), value.values.into()])
    }
}

pub fn parse(data: &[u8]) -> RespResult {
    match data[0] {
        b'+' => parse_simple_string(&data[1..]),
        b'-' => parse_simple_error(&data[1..]),
        b':' => parse_integer(&data[1..]),
        b'$' => parse_bulk_string(&data[1..]),
        b'*' => parse_array(&data[1..]),
        _ => Err(RespError::IncorrectFormat),
    }
}

fn parse_until_crlf(data: &[u8]) -> Result<(&[u8], &[u8]), RespError> {
    for (i, val) in data.iter().enumerate() {
        if val == &CR && data[i + 1] == LF {
            return Ok((&data[0..i], &data[i + 2..]));
        }
    }
    Err(RespError::NotEnoughBytes)
}

fn parse_simple_string(data: &[u8]) -> RespResult {
    let (result, leftovers) = parse_until_crlf(data)?;
    Ok((
        Resp::SimpleString(std::str::from_utf8(result).unwrap().to_string()),
        leftovers,
    ))
}

fn parse_simple_error(data: &[u8]) -> RespResult {
    let (result, leftovers) = parse_until_crlf(data)?;
    Ok((
        Resp::SimpleError(std::str::from_utf8(result).unwrap().to_string()),
        leftovers,
    ))
}

fn parse_integer(data: &[u8]) -> RespResult {
    let (result, leftovers) = parse_until_crlf(data)?;
    let number: i32 = std::str::from_utf8(result).unwrap().parse()?;
    Ok((Resp::Integer(number), leftovers))
}

fn parse_uinteger(data: &[u8]) -> Result<(usize, &[u8]), RespError> {
    let (result, leftovers) = parse_until_crlf(data)?;
    if data[0] == b'+' || data[0] == b'-' {
        return Err(RespError::IncorrectFormat);
    }

    let number: usize = std::str::from_utf8(result).unwrap().parse()?;
    Ok((number, leftovers))
}

fn parse_bulk_string(data: &[u8]) -> RespResult {
    let (n_items, content) = parse_uinteger(data)?;

    let (result, leftovers) = parse_until_crlf(content)?;
    if result.len() != n_items {
        return Err(RespError::IncorrectFormat);
    }

    Ok((
        Resp::BulkString(std::str::from_utf8(result).unwrap().to_string()),
        leftovers,
    ))
}

fn parse_array(data: &[u8]) -> RespResult {
    let (n_items, mut leftovers) = parse_uinteger(data)?;

    let mut results: Vec<Resp> = vec![];
    for _ in 0..n_items {
        let (result, temp_leftovers) = parse(leftovers)?;
        leftovers = temp_leftovers;
        results.push(result);
    }

    Ok((Resp::Array(results), leftovers))
}

pub fn parse_string_array(contents: Vec<Resp>) -> Result<Vec<String>, RespError> {
    let mut result: Vec<String> = vec![];
    for arg in contents {
        if let Resp::BulkString(val) = arg {
            result.push(val);
        } else {
            return Err(RespError::IncorrectFormat);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let test_data = b"+OK\r\n";
        let expecting = Resp::SimpleString("OK".to_string());

        let (result, _) = parse(test_data).unwrap();
        assert_eq!(result, expecting);
    }

    #[test]
    fn test_parse_simple_error() {
        let test_data = b"-ERROR\r\n";
        let expecting = Resp::SimpleError("ERROR".to_string());

        let (result, _) = parse(test_data).unwrap();
        assert_eq!(result, expecting);
    }

    #[test]
    fn test_parse_simple_integer() {
        let test_data: Vec<&[u8]> = vec![b":32\r\n", b":-32\r\n", b":+32\r\n"];
        let expecting: Vec<Resp> = vec![Resp::Integer(32), Resp::Integer(-32), Resp::Integer(32)];

        for (data, expected) in test_data.iter().zip(expecting.iter()) {
            let (result, _) = parse(data).unwrap();
            assert_eq!(result, *expected);
        }
    }

    #[test]
    fn test_parse_uinteger_success() {
        let test_data = b"32\r\n";
        let expecting_success: usize = 32;

        let (result, _) = parse_uinteger(test_data).unwrap();
        assert_eq!(result, expecting_success);
    }

    #[test]
    #[should_panic]
    fn test_parse_uinteger_failure() {
        let test_data: Vec<&[u8]> = vec![b":-32\r\n", b":+32\r\n"];

        for data in test_data {
            let (_, _) = parse_uinteger(data).unwrap();
        }
    }

    #[test]
    fn test_bulk_string() {
        let test_data = b"$5\r\nhello\r\n";
        let expecting = Resp::BulkString("hello".to_string());

        let (result, _) = parse(test_data).unwrap();
        assert_eq!(result, expecting)
    }

    #[test]
    fn test_array() {
        let test_data: Vec<&[u8]> = vec![
            b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n",
            b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n",
        ];

        let expecting: Vec<Resp> = vec![
            Resp::Array(vec![
                Resp::BulkString("hello".to_string()),
                Resp::BulkString("world".to_string()),
            ]),
            Resp::Array(vec![
                Resp::Integer(1),
                Resp::Integer(2),
                Resp::Integer(3),
                Resp::Integer(4),
                Resp::BulkString("hello".to_string()),
            ]),
            Resp::Array(vec![
                Resp::Array(vec![Resp::Integer(1), Resp::Integer(2), Resp::Integer(3)]),
                Resp::Array(vec![
                    Resp::SimpleString("Hello".to_string()),
                    Resp::SimpleError("World".to_string()),
                ]),
            ]),
        ];

        for (data, expected) in test_data.iter().zip(expecting.iter()) {
            let (result, _) = parse(data).unwrap();
            assert_eq!(result, *expected);
        }
    }

    #[test]
    fn test_writer() {}
}
