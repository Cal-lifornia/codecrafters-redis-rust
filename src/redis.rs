use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
};

use crate::{
    commands::{CommandError, CommandQueueList, RedisCommand},
    db::RedisDatabase,
    replication::{connect_to_host, read_host_connection},
    resp::{self, Resp, RespError},
    types::{Context, RedisInfo, Replica, ReplicationInfo},
};

pub async fn init(
    address: &str,
    port: &str,
    replica: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(format!("{address}:{port}")).await?;
    let db = Arc::new(RedisDatabase::default());
    let (is_master, role, host_addr) = if let Some(host) = replica {
        let host_option = host.split_whitespace().take(2).collect::<Vec<_>>();
        let host_name = if host_option[0] == "localhost" {
            "127.0.0.1"
        } else {
            host_option[0]
        };

        (false, "slave", format!("{}:{}", host_name, host_option[1]))
    } else {
        (true, "master", "".to_string())
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
    let replicas = Arc::new(RwLock::new(vec![]));

    if role == "slave" {
        let connection = match connect_to_host(host_addr, info.clone()).await {
            Ok(connection) => connection,
            Err(err) => {
                eprintln!("failed to connect to master; err = {err:?}");
                return Err(err.into());
            }
        };
        let stream = read_host_connection(connection, arc_info.clone()).await;
        let db_clone = db.clone();
        let info_clone = Arc::clone(&arc_info);
        let replicas_clone = replicas.clone();
        tokio::spawn(async move {
            handle_stream(
                stream,
                db_clone.clone(),
                Arc::new(Mutex::new(false)),
                Arc::new(Mutex::new(vec![])),
                info_clone.clone(),
                replicas_clone.clone(),
                is_master,
            )
            .await
        });
    }
    loop {
        let (socket, _) = listener.accept().await?;
        let db_clone = Arc::clone(&db);
        let queue_list = Arc::new(Mutex::new(vec![]));
        let queued = Arc::new(Mutex::new(false));
        let info_clone = Arc::clone(&arc_info);
        let replicas_clone = replicas.clone();
        tokio::spawn(async move {
            handle_stream(
                socket,
                db_clone,
                queued,
                queue_list,
                info_clone,
                replicas_clone.clone(),
                is_master,
            )
            .await
        });
    }
}

pub async fn handle_stream(
    stream: TcpStream,
    db: Arc<RedisDatabase>,
    queued: Arc<Mutex<bool>>,
    queue_list: CommandQueueList,
    info: Arc<RwLock<RedisInfo>>,
    replicas: Arc<RwLock<Vec<Replica>>>,
    master: bool,
) -> Result<(), RedisError> {
    let mut buf = [0; 1024];
    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(RwLock::new(writer));
    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) => {
                continue;
            }
            Ok(n) => n,
            Err(err) => {
                eprintln!("failed to read from socket; err = {err:?}");
                return Err(err.into());
            }
        };

        let ctx = Context::new(
            writer.clone(),
            db.clone(),
            queued.clone(),
            queue_list.clone(),
            info.clone(),
            master,
            replicas.clone(),
        );

        if let Err(err) = parse_input(&buf[0..n], &ctx).await {
            eprintln!("ran into error: {err:?}");
            continue;
        }
    }
}

async fn parse_input(buf: &[u8], ctx: &Context) -> Result<(), RedisError> {
    let mut input = buf;
    while let Ok((Resp::Array(contents), leftovers)) = resp::parse(input) {
        let cmd = RedisCommand::try_from(contents)?;
        // println!("got cmd; {cmd:#?}");
        match cmd.run_command_full(ctx).await {
            Ok(_) => {}
            Err(err) => {
                eprintln!("ERR {err}");
            }
        }
        input = leftovers;
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
