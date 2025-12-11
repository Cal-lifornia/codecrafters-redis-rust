use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{command::RedisCommand, database::RedisDatabase};

#[derive(Clone)]
pub struct Context {
    pub db: Arc<RedisDatabase>,
    pub transaction: Arc<RwLock<Option<Vec<RedisCommand>>>>,
}
