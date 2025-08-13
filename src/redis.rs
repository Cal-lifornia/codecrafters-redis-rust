use std::{io::Error, sync::Arc};

use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, RwLock},
};

use crate::{
    commands::parse_array_command,
    db::RedisDatabase,
    replication::connect_to_host,
    resp::{self, Resp},
    types::{Context, RedisInfo, ReplicationInfo},
};

pub async fn init(
    address: &str,
    port: &str,
    replica: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(format!("{address}:{port}")).await?;
    let db = Arc::new(RedisDatabase::default());
    let (role, host_addr) = if let Some(host) = replica {
        let host_option = host.split_whitespace().take(2).collect::<Vec<_>>();
        let host_name = if host_option[0] == "localhost" {
            "127.0.0.1"
        } else {
            host_option[0]
        };

        ("slave", format!("{}:{}", host_name, host_option[1]))
    } else {
        ("master", "".to_string())
    };
    let info = if role == "master" {
        RedisInfo {
            port: port.to_string(),
            replication: ReplicationInfo::new_master(role.to_string(), host_addr.clone(), 0),
        }
    } else {
        RedisInfo {
            port: port.to_string(),
            replication: ReplicationInfo::new_slave(role.to_string(), host_addr.clone()),
        }
    };

    if role == "slave" {
        if let Err(err) = connect_to_host(host_addr, info.clone()).await {
            eprintln!("failed to connect to master; err = {err:?}");
            return Err(err);
        }
    }

    let info = Arc::new(RwLock::new(info));

    loop {
        let (mut socket, _) = listener.accept().await?;
        let info_clone = Arc::clone(&info);
        let db_clone = Arc::clone(&db);
        let sender = db_clone.clone_sender();
        let queue_list = Arc::new(Mutex::new(vec![]));
        let queued = Arc::new(Mutex::new(false));

        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {e:?}");
                        return;
                    }
                };

                let (_, writer) = socket.split();

                let mut ctx = Context::new(
                    writer,
                    sender.clone(),
                    queued.clone(),
                    queue_list.clone(),
                    info_clone.clone(),
                );

                if let Err(err) = parse_input(&buf[0..n], &mut ctx).await {
                    eprintln!("ran into error: {err:?}");
                    return;
                }
            }
        });
        tokio::spawn(async move {
            if let Err(err) = db_clone.handle_receiver().await {
                eprintln!("ran into error: {err:?}");
            }
        });
    }
}

async fn parse_input<Writer>(buf: &[u8], ctx: &mut Context<Writer>) -> Result<(), std::io::Error>
where
    Writer: AsyncWrite + Unpin,
{
    if let (Resp::Array(contents), _) = resp::parse(buf).unwrap() {
        match parse_array_command(contents, ctx).await {
            Ok(_) => Ok(()),
            Err(err) => {
                ctx.out
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
