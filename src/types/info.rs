#[derive(Debug, Clone)]
pub struct RedisInfo {
    pub replication: ReplicationInfo,
    pub config: Config,
}

#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    pub role: String,
    pub host_address: String,
    pub offset: i32,
    pub replica_offset: i32,
    pub replication_id: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dir: String,
    pub db_filename: String,
    pub port: String,
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
            replica_offset: 0,
            replication_id,
        }
    }

    pub fn new_slave(role: String, host_id: String) -> Self {
        Self {
            role,
            host_address: host_id,
            offset: -1,
            replica_offset: 0,
            replication_id: "?".to_string(),
        }
    }
}
