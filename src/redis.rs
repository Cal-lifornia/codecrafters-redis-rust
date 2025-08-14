use std::sync::Arc;

use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Mutex, RwLock},
};

use crate::{
    commands::{parse_array_command, CommandError, RedisCommand},
    db::RedisDatabase,
    replication::{connect_to_host, read_host_connection},
    resp::{self, Resp, RespError},
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
    let arc_info = Arc::new(RwLock::new(info.clone()));
    let tcp_replica = Arc::new(Mutex::new(false));
    let (broadcaster, _) = broadcast::channel(8);

    if role == "slave" {
        match connect_to_host(host_addr, info.clone()).await {
            Ok(connection) => {
                read_host_connection(
                    connection,
                    db.clone_sender(),
                    Arc::new(Mutex::new(vec![])),
                    Arc::new(Mutex::new(false)),
                    arc_info.clone(),
                    tcp_replica.clone(),
                    broadcaster.clone(),
                )
                .await
            }
            Err(err) => {
                eprintln!("failed to connect to master; err = {err:?}");
                return Err(err);
            }
        }
    }

    loop {
        let (socket, _) = listener.accept().await?;
        let info_clone = Arc::clone(&arc_info);
        let db_clone = Arc::clone(&db);
        let sender = db_clone.clone_sender();
        let queue_list = Arc::new(Mutex::new(vec![]));
        let queued = Arc::new(Mutex::new(false));
        let replica_clone = Arc::clone(&tcp_replica);
        let broadcaster_clone = broadcaster.clone();

        tokio::spawn(async move {
            handle_stream(
                socket,
                sender.clone(),
                queue_list,
                queued,
                info_clone,
                replica_clone,
                broadcaster_clone,
            )
            .await
        });
        tokio::spawn(async move {
            if let Err(err) = db_clone.handle_receiver().await {
                eprintln!("ran into error: {err:?}");
            }
        });
    }
}

pub async fn handle_stream(
    mut socket: TcpStream,
    sender: mpsc::Sender<RedisCommand>,
    queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
    queued: Arc<Mutex<bool>>,
    info: Arc<RwLock<RedisInfo>>,
    tcp_replica: Arc<Mutex<bool>>,
    cmd_broadcaster: broadcast::Sender<Vec<Resp>>,
) -> Result<(), RedisError> {
    let mut buf = [0; 1024];
    loop {
        let n = match socket.read(&mut buf).await {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(err) => {
                eprintln!("failed to read from socket; err = {err:?}");
                return Err(err.into());
            }
        };

        let (_, writer) = socket.split();

        let mut ctx = Context::new(
            writer,
            sender.clone(),
            queued.clone(),
            queue_list.clone(),
            info.clone(),
            tcp_replica.clone(),
            cmd_broadcaster.clone(),
        );

        if let Err(err) = parse_input(&buf[0..n], &mut ctx).await {
            eprintln!("ran into error: {err:?}");
            return Err(err);
        }
    }
}

async fn parse_input<Writer>(buf: &[u8], ctx: &mut Context<Writer>) -> Result<(), RedisError>
where
    Writer: AsyncWrite + Unpin,
{
    if let (Resp::Array(contents), _) = resp::parse(buf)? {
        match parse_array_command(contents, ctx).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                ctx.out
                    .write_all(&Resp::SimpleError(format!("ERR {err}")).to_bytes())
                    .await?;
                return Ok(());
            }
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("{0}")]
    CommandError(#[from] CommandError),
    #[error("{0}")]
    ParseError(#[from] RespError),
    #[error("{0}")]
    IoError(#[from] std::io::Error),
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
