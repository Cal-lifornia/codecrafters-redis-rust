use anyhow::{Error, Result};
use std::io::Write;

use thiserror::Error;

pub type RespResult<'a> = std::result::Result<(Resp, &'a [u8]), RespError>;

#[derive(Debug, Error)]
pub enum RespError {
    #[error("not enough bytes")]
    NotEnoughBytes,
    #[error("incorrect format")]
    IncorrectFormat,
    #[error("{0}")]
    Other(Box<dyn std::error::Error>),
}

impl From<std::num::ParseIntError> for RespError {
    fn from(from: std::num::ParseIntError) -> Self {
        Self::Other(Box::new(from))
    }
}

impl From<std::io::Error> for RespError {
    fn from(from: std::io::Error) -> Self {
        Self::Other(Box::new(from))
    }
}

const CR: u8 = b'\r';
const LF: u8 = b'\n';

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Resp {
    SimpleString(String),
    SimpleError(String),
    Integer(i32),
    BulkString(String),
    Array(Vec<Resp>),
}

impl Resp {
    pub fn write_to_writer<W>(&self, writer: &mut W) -> Result<()>
    where
        W: Write,
    {
        match self {
            Resp::SimpleString(s) => {
                writer.write_all(b"+")?;
                writer.write_all(s.as_bytes())?;
                writer.write_all(&[CR, LF])?;
            }
            Resp::SimpleError(s) => {
                writer.write_all(b"-")?;
                writer.write_all(s.as_bytes())?;
                writer.write_all(&[CR, LF])?;
            }
            Resp::Integer(i) => {
                writer.write_all(b":")?;
                writer.write_all(format!("{}", i).as_bytes())?;
                writer.write_all(&[CR, LF])?;
            }
            Resp::BulkString(s) => {
                writer.write_all(b"$")?;
                writer.write_all(&s.len().to_ne_bytes())?;
                writer.write_all(&[CR, LF])?;
                writer.write_all(s.as_bytes())?;
                writer.write_all(&[CR, LF])?;
            }
            Resp::Array(v) => {
                writer.write_all(b"*")?;
                writer.write_all(&v.len().to_ne_bytes())?;
                writer.write_all(&[CR, LF])?;
                for resp in v {
                    resp.write_to_writer(writer)?;
                }
            }
        };
        Ok(())
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
}
