use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio::{
    io::AsyncWriteExt,
    net::tcp::OwnedWriteHalf,
    sync::{Mutex, RwLock},
};

use crate::{commands::CommandQueueList, db::RedisDatabase};

use super::RedisInfo;
pub struct Context {
    pub out: Arc<RwLock<OwnedWriteHalf>>,
    pub db: Arc<RedisDatabase>,
    pub queued: Arc<Mutex<bool>>,
    pub queue_list: CommandQueueList,
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
        db: Arc<RedisDatabase>,
        queued: Arc<Mutex<bool>>,
        queue_list: CommandQueueList,
        info: Arc<RwLock<RedisInfo>>,
        is_master: bool,
        replicas: Arc<RwLock<Vec<Replica>>>,
    ) -> Self {
        Self {
            out,
            db,
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
