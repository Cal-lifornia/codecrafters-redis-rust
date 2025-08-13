#[derive(Debug, Clone)]
pub struct RedisInfo {
    pub replication: Replication,
}

#[derive(Debug, Clone)]
pub struct Replication {
    pub role: String,
    pub connected_slaves: usize,
    pub replication_id: String,
    pub offset: usize,
}

impl Replication {
    pub fn new(role: String, connected_slaves: usize, offset: usize) -> Self {
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
            connected_slaves,
            replication_id,
            offset,
        }
    }
}
