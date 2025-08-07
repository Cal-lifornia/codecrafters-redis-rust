use std::{io::Error, sync::Arc};

use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

use crate::{
    commands::{parse_array_command, RedisCommand},
    context::Context,
    db::RedisDatabase,
    resp::{self, Resp},
};

pub async fn init(address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(address).await?;

    loop {
        let db = Arc::new(RedisDatabase::default());
        let sender = db.clone_sender();

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

                let (_, mut writer) = socket.split();

                if let Err(err) = parse_input(&mut writer, &buf[0..n], sender.clone()).await {
                    eprintln!("ran into error: {err:?}");
                    return;
                }
            }
        });
        tokio::spawn(async move {
            loop {
                if let Err(err) = db.handle_receiver().await {
                    eprintln!("ran into error: {err:?}");
                    return;
                }
            }
        });
    }
}

async fn parse_input<Writer>(
    out: &mut Writer,
    buf: &[u8],
    db_sender: mpsc::Sender<RedisCommand>,
) -> Result<(), std::io::Error>
where
    Writer: AsyncWrite + Unpin,
{
    if let (Resp::Array(contents), _) = resp::parse(buf).unwrap() {
        match parse_array_command(out, contents, &Context { db_sender }).await {
            Ok(_) => Ok(()),
            Err(err) => {
                out.write_all(&Resp::SimpleError(format!("ERR {err}")).to_bytes())
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
