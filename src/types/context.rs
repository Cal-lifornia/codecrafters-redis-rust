use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio::{
    io::AsyncWriteExt,
    net::tcp::OwnedWriteHalf,
    sync::{broadcast, mpsc, Mutex, RwLock},
};

use crate::{commands::RedisCommand, resp::Resp};

use super::RedisInfo;
pub struct Context {
    pub out: Arc<RwLock<OwnedWriteHalf>>,
    pub db_sender: mpsc::Sender<RedisCommand>,
    pub queued: Arc<Mutex<bool>>,
    pub queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
    pub info: Arc<RwLock<RedisInfo>>,
    pub replicas: Arc<RwLock<Vec<Replica>>>,
    pub is_master: bool,
}

pub struct Replica {
    pub replica: Arc<RwLock<OwnedWriteHalf>>,
}

impl Context {
    pub fn new(
        out: Arc<RwLock<OwnedWriteHalf>>,
        db_sender: mpsc::Sender<RedisCommand>,
        queued: Arc<Mutex<bool>>,
        queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
        info: Arc<RwLock<RedisInfo>>,
        is_master: bool,
        replicas: Arc<RwLock<Vec<Replica>>>,
    ) -> Self {
        Self {
            out,
            db_sender,
            queued,
            queue_list,
            info,
            replicas,
            is_master,
        }
    }

    pub async fn write_to_stream(&self, output: Bytes) -> Result<()> {
        self.out.write().await.write_all(&output).await?;
        Ok(())
    }
}
