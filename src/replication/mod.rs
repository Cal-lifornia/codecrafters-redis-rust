use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::TcpStream,
    sync::{broadcast, mpsc, Mutex, RwLock},
};

use crate::{commands::RedisCommand, mod_flat, redis::handle_stream, resp::Resp, types::RedisInfo};

mod_flat!(handshake);

pub async fn connect_to_host(host_address: String, info: RedisInfo) -> Result<TcpStream> {
    let mut connection = TcpStream::connect(host_address).await?;

    handshake(&mut connection, info).await?;

    Ok(connection)
}

pub async fn read_host_connection(
    connection: TcpStream,
    sender: mpsc::Sender<RedisCommand>,
    queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
    queued: Arc<Mutex<bool>>,
    info: Arc<RwLock<RedisInfo>>,
    tcp_replica: Arc<Mutex<bool>>,
    cmd_broadcaster: broadcast::Sender<Vec<Resp>>,
) {
    let mut reader = BufReader::new(connection);
    let mut file_size = String::new();
    let _ = reader.read_line(&mut file_size).await;
    let size = file_size
        .trim_start_matches("$")
        .trim_end()
        .parse::<usize>()
        .expect("should have a valid size");
    println!("slave: read RDB with size {size}");
    let mut buf = BytesMut::with_capacity(size);
    buf.resize(size, 0);
    let _ = reader.read_exact(&mut buf).await;
    println!("slave: RDB: {buf:?}");
    let stream = reader.into_inner();
    tokio::spawn(async move {
        handle_stream(
            stream,
            sender.clone(),
            queue_list.clone(),
            queued.clone(),
            info.clone(),
            tcp_replica,
            cmd_broadcaster,
        )
        .await
    });
}

#[derive(Debug, Error)]
pub enum ReplicaError {
    #[error("expecting {0}, got {1}")]
    UnexpectedResponse(String, String),
    #[error("error parsing input")]
    IncorrectFormat,
}
