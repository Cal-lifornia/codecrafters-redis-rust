#[derive(Debug, Clone)]
pub struct RedisInfo {
    pub port: String,
    pub replication: ReplicationInfo,
}

#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    pub role: String,
    pub host_id: String,
    pub connected_slaves: usize,
    pub offset: usize,
    pub replication_id: String,
}

impl ReplicationInfo {
    pub fn new(role: String, host_id: String, connected_slaves: usize, offset: usize) -> Self {
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
            host_id,
            connected_slaves,
            offset,
            replication_id,
        }
    }
}
