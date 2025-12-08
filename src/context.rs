use std::sync::Arc;

use crate::database::RedisDatabase;

#[derive(Clone)]
pub struct Context {
    pub db: Arc<RedisDatabase>,
}
