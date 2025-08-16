use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    sync::RwLock,
};

use crate::{mod_flat, types::RedisInfo};

mod_flat!(handshake);

pub async fn connect_to_host(host_address: String, info: RedisInfo) -> Result<TcpStream> {
    let mut connection = TcpStream::connect(host_address).await?;

    handshake(&mut connection, info).await?;

    Ok(connection)
}

pub async fn read_host_connection(
    connection: TcpStream,
    info: Arc<RwLock<RedisInfo>>,
) -> TcpStream {
    let mut reader = BufReader::new(connection);

    let mut full_resync = String::new();
    let _ = reader.read_line(&mut full_resync).await;

    {
        // let offset: Vec<i32> = full_resync
        //     .split_whitespace()
        //     .filter_map(|val| val.parse::<i32>().ok())
        //     .collect();
        drop(full_resync);
        info.write().await.replication.offset = 0;
    }
    let mut file_size = String::new();
    let _ = reader.read_line(&mut file_size).await;
    drop(file_size);
    // println!("file_size: {file_size}");
    // let size = file_size
    //     .trim_start_matches("$")
    //     .trim_end()
    //     .parse::<usize>()
    //     .expect("should have a valid size");
    // println!("slave: read RDB with size {size}");
    // let mut buf = BytesMut::with_capacity(size);
    // buf.resize(size, 0);
    // let _ = reader.read_exact(&mut buf).await;
    // println!("slave: RDB: {buf:?}");
    reader.into_inner()
}

#[derive(Debug, Error)]
pub enum ReplicaError {
    #[error("expecting {0}, got {1}")]
    UnexpectedResponse(String, String),
    #[error("error parsing input")]
    IncorrectFormat,
}
