use std::sync::Arc;

use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct RedisInfo {
    pub port: String,
    pub replication: ReplicationInfo,
}

#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    pub role: String,
    pub host_address: String,
    pub offset: i32,
    pub replication_id: String,
}

impl ReplicationInfo {
    pub fn new_master(role: String, host_address: String, offset: i32) -> Self {
        let replication_id: String = if role == "master" {
            const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789";
            const ID_LEN: usize = 40;
            (0..ID_LEN)
                .map(|_| {
                    let idx = rand::random_range(0..CHARSET.len());
                    CHARSET[idx] as char
                })
                .collect()
        } else {
            "".to_string()
        };
        Self {
            role,
            host_address,
            offset,
            replication_id,
        }
    }

    pub fn new_slave(role: String, host_id: String) -> Self {
        Self {
            role,
            host_address: host_id,
            offset: -1,
            replication_id: "?".to_string(),
        }
    }
}
