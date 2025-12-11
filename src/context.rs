use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{command::AsyncCommand, database::RedisDatabase};

#[derive(Clone)]
pub struct Context {
    pub db: Arc<RedisDatabase>,
    pub multi: Arc<RwLock<Option<TransactionList>>>,
}

pub type TransactionList = Vec<Box<dyn AsyncCommand + Sync + Send>>;
