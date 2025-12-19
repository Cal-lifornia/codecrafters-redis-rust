use bytes::Bytes;
use tokio::io::AsyncBufRead;

use crate::resp::RespType;

impl RespType {
    pub async fn async_read<R: AsyncBufRead + std::marker::Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        use std::io::Error;
        use std::io::ErrorKind;
        use tokio::io::AsyncBufReadExt;

        let mut buf = String::new();
        let _ = reader.read_line(&mut buf).await?;

        match buf.chars().next() {
            Some('+') => Ok(Self::SimpleString(Bytes::copy_from_slice(
                buf.trim_start_matches('+').trim_end().as_bytes(),
            ))),
            Some('-') => Ok(Self::SimpleError(Bytes::copy_from_slice(
                buf.trim_start_matches('-').as_bytes(),
            ))),
            Some(':') => Ok(Self::Integer(
                buf.trim_start_matches(':')
                    .trim_end()
                    .parse()
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::InvalidData,
                            format!("expected valid integer: {err}"),
                        )
                    })?,
            )),
            Some('$') => {
                let size = buf
                    .trim_start_matches("$")
                    .trim_end()
                    .parse::<i64>()
                    .map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("should have a valid file size; {err}"),
                        )
                    })?;
                if size == -1 {
                    Ok(Self::NullBulkString)
                } else if size >= 0 {
                    let mut contents = String::new();
                    let _ = reader.read_line(&mut contents).await?;
                    Ok(Self::BulkString(Bytes::copy_from_slice(
                        contents.trim_end().as_bytes(),
                    )))
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidData,
                        "invalid bulk string length",
                    ))
                }
            }
            Some('*') => {
                let size = buf
                    .trim_start_matches("*")
                    .trim_end()
                    .parse::<i64>()
                    .map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("should have a valid file size; {err}"),
                        )
                    })?;
                if size == -1 {
                    Ok(Self::NullArray)
                } else if size >= 0 {
                    let mut contents = vec![];
                    for _ in 0..size {
                        let fut = Box::pin(RespType::async_read(reader));
                        contents.push(fut.await?);
                    }
                    Ok(Self::Array(contents))
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidData,
                        "invalid bulk array length",
                    ))
                }
            }
            Some(ch) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("'{ch}' is an invalid first byte"),
            )),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "empty reader",
            )),
        }
    }
}
