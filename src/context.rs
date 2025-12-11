use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::{command::RedisCommand, database::RedisDatabase};

#[derive(Clone)]
pub struct Context {
    pub db: Arc<RedisDatabase>,
    pub transactions: Arc<RwLock<Option<Vec<RedisCommand>>>>,
    // pub buffer: Arc<Mutex<BytesMut>>,
}
