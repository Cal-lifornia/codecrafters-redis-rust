use std::io::Error;

use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

use crate::{
    commands::{parse_array_command, RedisCommand},
    db::RedisDatabase,
    resp::{self, Resp},
};

pub async fn init(address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(address).await?;

    loop {
        let db = RedisDatabase::default();

        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let n = match socket.try_read(&mut buf) {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {e:?}");
                        return;
                    }
                };

                let sender = db.clone_sender();

                let (_, mut writer) = socket.split();

                if let Err(err) = parse_input(&mut writer, &buf[0..n], sender).await {
                    eprintln!("ran into error: {err:?}");
                    return;
                }
            }
        });
    }
}

async fn parse_input<'a, Writer>(
    writer: &mut Writer,
    buf: &[u8],
    sender: mpsc::Sender<RedisCommand<'a>>,
) -> Result<(), std::io::Error>
where
    Writer: AsyncWrite + Unpin,
{
    if let (Resp::Array(contents), _) = resp::parse(buf).unwrap() {
        match parse_array_command(contents, sender).await {
            Ok(result) => writer.write_all(&result.to_bytes()).await,
            Err(err) => {
                writer
                    .write_all(&Resp::SimpleError(format!("ERR {err}")).to_bytes())
                    .await
            }
        }
    } else {
        Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid input",
        ))
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{collections::VecDeque, io::Read};

//     use super::*;

//     // #[test]
//     // fn test_handle_stream() {
//     //     let test_input = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
//     //     let expected = b"$3\r\nhey\r\n";

//     //     let mut writer = VecDeque::new();

//     //     parse_input(&mut writer, test_input).unwrap();

//     //     let mut results = [0u8; 9];
//     //     writer.read_exact(&mut results).unwrap();
//     //     assert_eq!(results, *expected);
//     // }

//     // #[test]
//     // fn test_parse_command() {
//     //     let test_input = vec![
//     //         Resp::BulkString("echo".to_string()),
//     //         Resp::BulkString("hey".to_string()),
//     //     ];
//     //     let expected = b"$3\r\nhey\r\n";
//     //     let mut writer = VecDeque::new();

//     //     parse_array_command(&test_input, &mut writer).unwrap();

//     //     let mut results = [0u8; 9];
//     //     writer.read_exact(&mut results).unwrap();
//     //     assert_eq!(results, *expected);
//     // }
// }
