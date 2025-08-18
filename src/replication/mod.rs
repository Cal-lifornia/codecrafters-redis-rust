use std::sync::Arc;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream},
    net::TcpStream,
    sync::RwLock,
};

use crate::{resp::Resp, types::RedisInfo};

pub async fn get_host_connection(host_address: String) -> Result<TcpStream, std::io::Error> {
    let connection = TcpStream::connect(host_address).await?;

    Ok(connection)
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
    streamer.flush().await?;

    let _ = streamer.read_line(&mut buf).await?;
    streamer.flush().await?;
    // println!("first buf {buf}");
    buf.clear();

    streamer
        .write_all(
            &Resp::bulk_string_array(&[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"listening-port"),
                Bytes::from(info.read().await.config.port.clone()),
            ])
            .to_bytes(),
        )
        .await?;
    streamer.flush().await?;

    let _ = streamer.read_line(&mut buf).await;
    // println!("second buf {buf}");
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
    streamer.flush().await?;

    let _ = streamer.read_line(&mut buf).await;

    // println!("third buf {buf}");
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
    streamer.flush().await?;

    let _ = streamer.read_line(&mut buf).await;
    {
        let offset: Vec<i32> = buf
            .split_whitespace()
            .filter_map(|val| val.parse::<i32>().ok())
            .collect();
        info.write().await.replication.offset = offset[0];
    }
    buf.clear();

    let mut file_size = String::new();
    let _ = streamer.read_line(&mut file_size).await;
    // println!("file_size: {file_size}");
    let size = file_size
        .trim_start_matches("$")
        .trim_end()
        .parse::<usize>()
        .expect("should have a valid size");
    // println!("slave: read RDB with size {size}");
    let mut buf = BytesMut::with_capacity(size);
    buf.resize(size, 0);
    let _ = streamer.read_exact(&mut buf).await?;
    // println!("slave: RDB: {buf:?}");

    let leftovers = streamer.fill_buf().await.expect("expect buf").to_vec();
    println!(
        "leftovers {:#?}",
        leftovers.iter().map(|val| *val as char).collect::<String>()
    );

    // let mut leftovers = [0u8; 1024].to_vec();
    // let n = match streamer.read(&mut leftovers).await {
    //     Ok(0) => 0,
    //     Ok(n) => n,
    //     Err(err) => return Err(err),
    // };
    // println!("leftovers; {leftovers:#?}");
    Ok((streamer.into_inner(), leftovers))
}

#[derive(Debug, Error)]
pub enum ReplicaError {
    #[error("expecting {0}, got {1}")]
    UnexpectedResponse(String, String),
    #[error("error parsing input")]
    IncorrectFormat,
}
