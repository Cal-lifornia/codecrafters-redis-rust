use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::RwLock,
};

use crate::{
    command::get_command,
    redis_stream::RedisStream,
    resp::{RedisWrite, RespType},
    server::RedisError,
};

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("{0}")]
    HandshakeError(&'static str),
}

#[derive(Default, Clone)]
pub struct MainServer {
    replicas: Arc<RwLock<Option<Vec<TcpStream>>>>,
}

#[derive(Clone)]
pub struct Replica {
    main_reader: Arc<RwLock<OwnedReadHalf>>,
    main_writer: Arc<RwLock<OwnedWriteHalf>>,
}

impl Replica {
    pub async fn new(main_address: String) -> std::io::Result<Self> {
        println!("MAIN_ADDRESS: {main_address}");
        let stream = TcpStream::connect(main_address).await?;
        // stream.write_all(b"TEST").await?;
        let (main_reader, main_writer) = stream.into_split();
        Ok(Self {
            main_reader: Arc::new(RwLock::new(main_reader)),
            main_writer: Arc::new(RwLock::new(main_writer)),
        })
    }
    pub async fn handshake(&self) -> Result<(), RedisError> {
        {
            let mut writer = self.main_writer.write().await;
            let mut write_buf = BytesMut::new();
            println!("WRITING TO MAIN");
            vec![(Bytes::from("PING"))].write_to_buf(&mut write_buf);
            writer.write_all(&write_buf).await?;
            writer.flush().await?;
        }
        let (resp, _) = RespType::from_utf8(&self.read_main().await?)?;
        if resp != RespType::simple_string("PONG") {
            return Err(ReplicaError::HandshakeError("Didn't receive PONG").into());
        }

        // let mut redis_stream = RedisStream::try_from(resp)?;

        // if let Some(next) = redis_stream.next() {
        //     if next.to_ascii_lowercase().as_slice() != b"pong" {
        //     }
        // } else {
        //     return Err(ReplicaError::HandshakeError("Didn't receive PONG").into());
        // }

        Ok(())
    }

    pub async fn read_main(&self) -> std::io::Result<Bytes> {
        let mut reader = self.main_reader.write().await;
        let mut buf = [0u8; 1024];
        let n = reader.read(&mut buf).await?;
        Ok(Bytes::copy_from_slice(&buf[0..n]))
    }
}
