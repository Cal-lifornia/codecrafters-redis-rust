use std::{ops::Deref, sync::Arc};

use bytes::{Bytes, BytesMut};
use rand::{Rng, distr::Alphanumeric};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::RwLock,
};

use crate::{
    resp::{RedisWrite, RespType},
    server::RedisError,
};
pub struct ReplicationInfo {
    pub role: String,
    pub replication_id: String,
    pub offset: i64,
}

impl ReplicationInfo {
    pub fn new(master: bool) -> Self {
        let role = if master { "master" } else { "slave" };
        if master {
            let replication_id = (0..40)
                .map(|_| rand::rng().sample(Alphanumeric) as char)
                .collect();
            Self {
                role: role.into(),
                replication_id,
                offset: 0,
            }
        } else {
            Self {
                role: role.into(),
                replication_id: "?".into(),
                offset: -1,
            }
        }
    }
}

impl RedisWrite for ReplicationInfo {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut) {
        let output = format!(
            "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
            self.role, self.replication_id, self.offset
        );

        RespType::BulkString(Bytes::from(output)).write_to_buf(buf);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("{0}")]
    HandshakeError(&'static str),
}

#[derive(Default, Clone)]
pub struct MainServer {
    pub replicas: Arc<RwLock<Vec<Arc<RwLock<OwnedWriteHalf>>>>>,
}

#[derive(Clone)]
pub struct Replica {
    main_reader: Arc<RwLock<OwnedReadHalf>>,
    main_writer: Arc<RwLock<OwnedWriteHalf>>,
    port: String,
}

impl Replica {
    pub async fn new(main_address: String, port: String) -> std::io::Result<Self> {
        println!("MAIN_ADDRESS: {main_address}");
        let stream = TcpStream::connect(main_address).await?;
        // stream.write_all(b"TEST").await?;
        let (main_reader, main_writer) = stream.into_split();
        Ok(Self {
            main_reader: Arc::new(RwLock::new(main_reader)),
            main_writer: Arc::new(RwLock::new(main_writer)),
            port,
        })
    }
    pub async fn handshake(&self, info: &mut ReplicationInfo) -> Result<(), RedisError> {
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

        // Write first REPLCONF
        {
            let mut writer = self.main_writer.write().await;
            let mut write_buf = BytesMut::new();
            println!("WRITING TO MAIN");
            vec![
                (Bytes::from("REPLCONF")),
                Bytes::from("listening-port"),
                Bytes::from(self.port.clone()),
            ]
            .write_to_buf(&mut write_buf);
            writer.write_all(&write_buf).await?;
            writer.flush().await?;
        }

        let (resp, _) = RespType::from_utf8(&self.read_main().await?)?;
        if resp != RespType::simple_string("OK") {
            return Err(ReplicaError::HandshakeError("Didn't receive OK").into());
        }

        // Write second REPLCONF
        {
            let mut writer = self.main_writer.write().await;
            let mut write_buf = BytesMut::new();
            println!("WRITING TO MAIN");
            vec![
                (Bytes::from("REPLCONF")),
                Bytes::from("capa"),
                Bytes::from("psync2"),
            ]
            .write_to_buf(&mut write_buf);
            writer.write_all(&write_buf).await?;
            writer.flush().await?;
        }
        // Write PSYNC
        {
            let mut writer = self.main_writer.write().await;
            let mut write_buf = BytesMut::new();
            println!("WRITING TO MAIN");
            vec![
                (Bytes::from("PSYNC")),
                Bytes::from(info.replication_id.clone()),
                Bytes::from(info.offset.to_string()),
            ]
            .write_to_buf(&mut write_buf);
            writer.write_all(&write_buf).await?;
            writer.flush().await?;
        }

        let (resp, _) = RespType::from_utf8(&self.read_main().await?)?;
        if let RespType::SimpleString(psync) = resp {
            let results = String::from_utf8_lossy(&psync);
            let args: Vec<&str> = results.split_terminator(' ').collect();
            if let Some(repl_id) = &args.get(1) {
                info.replication_id = repl_id.to_string();
            }
        }
        Ok(())
    }

    pub async fn read_main(&self) -> std::io::Result<Bytes> {
        let mut reader = self.main_reader.write().await;
        let mut buf = [0u8; 1024];
        let n = reader.read(&mut buf).await?;
        Ok(Bytes::copy_from_slice(&buf[0..n]))
    }
}
