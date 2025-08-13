use std::sync::Arc;

use thiserror::Error;
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex, RwLock},
};

use crate::{commands::RedisCommand, mod_flat, redis::handle_stream, resp::Resp, types::RedisInfo};

mod_flat!(handshake);

pub async fn connect_to_host(
    host_address: String,
    info: RedisInfo,
) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let mut connection = match TcpStream::connect(host_address).await {
        Ok(stream) => stream,
        Err(err) => return Err(Box::new(err)),
    };

    handshake(&mut connection, info).await?;
    Ok(connection)
}

pub async fn read_host_connection(
    connection: TcpStream,
    sender: mpsc::Sender<RedisCommand>,
    queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
    queued: Arc<Mutex<bool>>,
    info: Arc<RwLock<RedisInfo>>,
) {
    tokio::spawn(async move {
        handle_stream(
            connection,
            sender.clone(),
            queue_list.clone(),
            queued.clone(),
            info.clone(),
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
