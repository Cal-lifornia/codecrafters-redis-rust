use std::sync::Arc;

use bytes::Bytes;
use either::Either;
use rand::{Rng, distr::Alphanumeric};
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
    pub role: Either<MainServer, Replica>,
    // pub buffer: Arc<Mutex<BytesMut>>,
}
