use std::sync::Arc;

use tokio::{
    io::AsyncWrite,
    sync::{broadcast, mpsc, Mutex, RwLock},
};

use crate::{commands::RedisCommand, resp::Resp};

use super::RedisInfo;
pub struct Context<Writer: AsyncWrite + Unpin> {
    pub out: Writer,
    pub db_sender: mpsc::Sender<RedisCommand>,
    pub queued: Arc<Mutex<bool>>,
    pub queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
    pub info: Arc<RwLock<RedisInfo>>,
    pub tcp_replica: Arc<Mutex<bool>>,
    pub cmd_broadcaster: broadcast::Sender<Vec<Resp>>,
}

impl<Writer: AsyncWrite + Unpin> Context<Writer> {
    pub fn new(
        out: Writer,
        db_sender: mpsc::Sender<RedisCommand>,
        queued: Arc<Mutex<bool>>,
        queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
        info: Arc<RwLock<RedisInfo>>,
    ) -> Self {
        let (broadcaster, _) = broadcast::channel(8);
        Self {
            out,
            db_sender,
            queued,
            queue_list,
            info,
            tcp_replica: Arc::new(Mutex::new(false)),
            cmd_broadcaster: broadcaster,
        }
    }
}
