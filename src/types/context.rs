use std::sync::Arc;

use tokio::{
    io::AsyncWrite,
    sync::{mpsc, Mutex, RwLock},
};

use crate::{commands::RedisCommand, resp::Resp};

use super::RedisInfo;
pub struct Context<Writer: AsyncWrite + Unpin> {
    pub out: Writer,
    pub db_sender: mpsc::Sender<RedisCommand>,
    pub queued: Arc<Mutex<bool>>,
    pub queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
    pub info: Arc<RwLock<RedisInfo>>,
}

impl<Writer: AsyncWrite + Unpin> Context<Writer> {
    pub fn new(
        out: Writer,
        db_sender: mpsc::Sender<RedisCommand>,
        queued: Arc<Mutex<bool>>,
        queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
        info: Arc<RwLock<RedisInfo>>,
    ) -> Self {
        Self {
            out,
            db_sender,
            queued,
            queue_list,
            info,
        }
    }
}
