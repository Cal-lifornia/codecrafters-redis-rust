use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::RwLock;

use crate::{
    command::RedisCommand,
    database::RedisDatabase,
    resp::{RedisWrite, RespType},
};

#[derive(Clone)]
pub struct Context {
    pub db: Arc<RedisDatabase>,
    pub transactions: Arc<RwLock<Option<Vec<RedisCommand>>>>,
    pub replication: Arc<RwLock<ReplicationInfo>>,
    // pub buffer: Arc<Mutex<BytesMut>>,
}

pub struct ReplicationInfo {
    pub role: String,
}

impl RedisWrite for ReplicationInfo {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut) {
        // RespType::BulkString(Bytes::from("# Replication")).write_to_buf(buf);
        RespType::BulkString(Bytes::from(format!("role:{}", self.role.to_lowercase())))
            .write_to_buf(buf);
    }
}
