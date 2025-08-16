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
    pub app_info: Arc<RwLock<RedisInfo>>,
    pub replicas: Arc<RwLock<Vec<Replica>>>,
    pub ctx_info: CtxInfo,
}

#[derive(Clone, Copy)]
pub struct CtxInfo {
    // Is the current running app a master redis server or slave
    pub is_master: bool,
    // Is the stream being read using the master connection?
    // For updating offset specifically
    pub stream_from_master: bool,
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
        app_info: Arc<RwLock<RedisInfo>>,
        replicas: Arc<RwLock<Vec<Replica>>>,
        ctx_info: CtxInfo,
    ) -> Self {
        Self {
            out,
            db,
            queued,
            queue_list,
            app_info,
            replicas,
            ctx_info,
        }
    }

    pub async fn write_to_stream(&self, output: Bytes) -> Result<()> {
        self.out.write().await.write_all(&output).await?;
        Ok(())
    }
}
