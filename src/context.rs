use tokio::sync::mpsc;

use crate::commands::RedisCommand;

pub struct Context {
    pub db_sender: mpsc::Sender<RedisCommand>,
}
