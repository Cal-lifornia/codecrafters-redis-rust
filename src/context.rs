use std::sync::Arc;

use bytes::BytesMut;
use either::Either;
use tokio::{
    net::{TcpStream, tcp::OwnedWriteHalf},
    sync::RwLock,
};

use crate::{
    command::RedisCommand,
    database::RedisDatabase,
    replica::{MainServer, Replica, ReplicationInfo},
};

#[derive(Clone)]
pub struct Context {
    pub db: Arc<RedisDatabase>,
    pub writer: Arc<RwLock<OwnedWriteHalf>>,
    pub transactions: Arc<RwLock<Option<Vec<RedisCommand>>>>,
    pub replication: Arc<RwLock<ReplicationInfo>>,
    #[allow(unused)]
    pub role: Either<MainServer, Replica>,
    // pub buffer: Arc<Mutex<BytesMut>>,
}

#[derive(Clone)]
pub struct Connection {
    stream: Arc<RwLock<TcpStream>>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Arc::new(RwLock::new(stream)),
            buffer: BytesMut::new(),
        }
    }
}
