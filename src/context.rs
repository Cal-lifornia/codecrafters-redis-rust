use std::sync::Arc;

use bytes::Bytes;
use rand::{Rng, distr::Alphanumeric};
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
    pub replication_id: String,
    pub offset: i64,
}

impl ReplicationInfo {
    pub fn new(master: bool) -> Self {
        let role = if master { "master" } else { "slave" };
        let replication_id = (0..40)
            .map(|_| rand::rng().sample(Alphanumeric) as char)
            .collect();
        Self {
            role: role.into(),
            replication_id,
            offset: 0,
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
