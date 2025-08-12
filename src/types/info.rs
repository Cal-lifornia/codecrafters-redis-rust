#[derive(Debug, Clone)]
pub struct RedisInfo {
    pub replication: Replication,
}

#[derive(Debug, Clone)]
pub struct Replication {
    pub role: String,
    pub connected_slaves: usize,
}

impl Default for Replication {
    fn default() -> Self {
        Self {
            role: "master".into(),
            connected_slaves: 0,
        }
    }
}
