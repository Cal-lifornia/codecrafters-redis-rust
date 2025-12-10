use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use hashbrown::HashMap;
use indexmap::IndexMap;
use tokio::sync::RwLock;

use crate::{
    database::{DatabaseString, ListBlocker, StreamBlocker},
    id::Id,
};

pub type DB<T> = Arc<RwLock<hashbrown::HashMap<Bytes, T>>>;

pub type Blocklist<T> = Arc<tokio::sync::Mutex<HashMap<Bytes, T>>>;

#[derive(Default)]
pub struct RedisDatabase {
    pub(crate) strings: DB<DatabaseString>,
    // pub(crate) nums: DB<i32>,
    pub(crate) streams: DB<IndexMap<Id, HashMap<Bytes, Bytes>>>,
    pub(crate) lists: DB<VecDeque<Bytes>>,
    pub(crate) list_blocklist: Blocklist<Vec<ListBlocker>>,
    pub(crate) stream_blocklist: Blocklist<IndexMap<Id, StreamBlocker>>,
}

impl RedisDatabase {
    pub async fn db_type(&self, key: &Bytes) -> Bytes {
        let value = if self.strings.read().await.contains_key(key) {
            "string"
        } else if self.streams.read().await.contains_key(key) {
            "stream"
        } else if self.lists.read().await.contains_key(key) {
            "list"
        } else {
            "none"
        };
        Bytes::from_static(value.as_bytes())
    }
}
