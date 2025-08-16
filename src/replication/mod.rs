use std::sync::Arc;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufStream},
    net::TcpStream,
    sync::RwLock,
};

use crate::{mod_flat, resp::Resp, types::RedisInfo};

mod_flat!(handshake);

pub async fn connect_to_host(host_address: String, info: RedisInfo) -> Result<TcpStream> {
    let mut connection = TcpStream::connect(host_address).await?;

    handshake(&mut connection, info).await?;

    Ok(connection)
}

pub async fn get_host_connection(host_address: String) -> Result<TcpStream, std::io::Error> {
    let connection = TcpStream::connect(host_address).await?;

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
    println!("leftovers; {:#?}", reader.buffer());
    reader.into_inner()
}
pub async fn handle_handshake(
    connection: TcpStream,
    info: Arc<RwLock<RedisInfo>>,
) -> Result<(TcpStream, Vec<u8>), std::io::Error> {
    let mut streamer = BufStream::new(connection);
    let mut buf = String::new();

    streamer
        .write_all(&Resp::bulk_string_array(&[Bytes::from_static(b"PING")]).to_bytes())
        .await?;
    let _ = streamer.read_line(&mut buf).await;
    buf.clear();

    streamer
        .write_all(
            &Resp::bulk_string_array(&[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"listening-port"),
                Bytes::from(info.read().await.port.clone()),
            ])
            .to_bytes(),
        )
        .await?;

    let _ = streamer.read_line(&mut buf).await;
    buf.clear();

    streamer
        .write_all(
            &Resp::bulk_string_array(&[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"psync2"),
            ])
            .to_bytes(),
        )
        .await?;

    let _ = streamer.read_line(&mut buf).await;
    buf.clear();

    streamer
        .write_all(
            &Resp::bulk_string_array(&[
                Bytes::from_static(b"PSYNC"),
                Bytes::from(info.read().await.replication.replication_id.clone()),
                Bytes::from(info.read().await.replication.offset.to_string()),
            ])
            .to_bytes(),
        )
        .await?;

    let _ = streamer.read_line(&mut buf).await;
    buf.clear();

    let mut file_size = String::new();
    let _ = streamer.read_line(&mut file_size).await;
    println!("file_size: {file_size}");
    let size = file_size
        .trim_start_matches("$")
        .trim_end()
        .parse::<usize>()
        .expect("should have a valid size");
    println!("slave: read RDB with size {size}");
    let mut buf = BytesMut::with_capacity(size);
    buf.resize(size, 0);
    let _ = streamer.read_exact(&mut buf).await;
    println!("slave: RDB: {buf:?}");
    // let leftovers = streamer.buffer().to_vec();

    let mut leftovers = vec![];
    let _ = streamer.read_to_end(&mut leftovers).await;
    println!("leftovers; {leftovers:#?}");
    Ok((streamer.into_inner(), leftovers))
}

#[derive(Debug, Error)]
pub enum ReplicaError {
    #[error("expecting {0}, got {1}")]
    UnexpectedResponse(String, String),
    #[error("error parsing input")]
    IncorrectFormat,
}
