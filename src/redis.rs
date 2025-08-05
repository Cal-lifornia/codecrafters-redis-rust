use std::sync::Arc;

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};

use crate::{
    commands::parse_array_command,
    db::RedisDatabase,
    resp::{self, Resp},
    RedisError,
};

pub async fn init(address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(address).await?;

    loop {
        let db = Arc::new(RedisDatabase::default());
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let n = match socket.try_read(&mut buf) {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                let (reader, mut writer) = socket.split();

                let clone_db = db.clone();

                if let Err(err) = parse_input(reader, &mut writer, &buf[0..n], clone_db).await {
                    eprintln!("error {:?}", err);
                    return;
                }
            }
        });
    }
}

// pub fn handle_stream<S>(stream: &mut S, db: Arc<RedisDatabase>) -> Result<(), RedisError>
// where
//     S: std::io::Write + std::io::Read + tokio::io::AsyncWrite + tokio::io::AsyncRead,
// {
//     let mut buf = [0; 512];
//     loop {
//         if let Ok(count) = stream.read(&mut buf) {
//             if count == 0 {
//                 break;
//             }
//             parse_input(stream, &buf, db.clone())?
//         }
//     }
//     Ok(())
// }

async fn parse_input<Reader, Writer>(
    _reader: Reader,
    writer: &mut Writer,
    buf: &[u8],
    db: Arc<RedisDatabase>,
) -> Result<(), RedisError>
where
    Reader: AsyncRead + Unpin,
    Writer: AsyncWrite + Unpin,
{
    match resp::parse(buf) {
        Ok((input, _)) => match input {
            Resp::Array(contents) => match parse_array_command(&contents, db) {
                Ok(response) => response.write_to_async_writer(writer).await?,
                Err(err) => {
                    Resp::SimpleError(format!("ERR {err}"))
                        .write_to_async_writer(writer)
                        .await?
                }
            },
            _ => {
                Resp::SimpleError("ERR invalid command".to_string())
                    .write_to_async_writer(writer)
                    .await?
            }
        },

        Err(err) => {
            Resp::SimpleError(format!("ERR {err}"))
                .write_to_async_writer(writer)
                .await?
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, io::Read};

    use super::*;

    // #[test]
    // fn test_handle_stream() {
    //     let test_input = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
    //     let expected = b"$3\r\nhey\r\n";

    //     let mut writer = VecDeque::new();

    //     parse_input(&mut writer, test_input).unwrap();

    //     let mut results = [0u8; 9];
    //     writer.read_exact(&mut results).unwrap();
    //     assert_eq!(results, *expected);
    // }

    // #[test]
    // fn test_parse_command() {
    //     let test_input = vec![
    //         Resp::BulkString("echo".to_string()),
    //         Resp::BulkString("hey".to_string()),
    //     ];
    //     let expected = b"$3\r\nhey\r\n";
    //     let mut writer = VecDeque::new();

    //     parse_array_command(&test_input, &mut writer).unwrap();

    //     let mut results = [0u8; 9];
    //     writer.read_exact(&mut results).unwrap();
    //     assert_eq!(results, *expected);
    // }
}
