use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::TcpStream,
    sync::{Mutex, RwLock},
};

use crate::{
    commands::CommandQueueList,
    db::RedisDatabase,
    mod_flat,
    redis::handle_stream,
    types::{RedisInfo, Replica},
};

mod_flat!(handshake);

pub async fn connect_to_host(host_address: String, info: RedisInfo) -> Result<TcpStream> {
    let mut connection = TcpStream::connect(host_address).await?;

    handshake(&mut connection, info).await?;

    Ok(connection)
}

pub async fn read_host_connection(
    connection: TcpStream,
    db: Arc<RedisDatabase>,
    queued: Arc<Mutex<bool>>,
    queue_list: CommandQueueList,
    info: Arc<RwLock<RedisInfo>>,
    replicas: Arc<RwLock<Vec<Replica>>>,
    is_replica: bool,
) {
    let mut reader = BufReader::new(connection);

    let mut full_resync = String::new();
    let _ = reader.read_line(&mut full_resync).await;
    drop(full_resync);

    let mut file_size = String::new();
    let _ = reader.read_line(&mut file_size).await;
    println!("file_size: {file_size}");
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
            db,
            queued.clone(),
            queue_list.clone(),
            info.clone(),
            replicas.clone(),
            is_replica,
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
