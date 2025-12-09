use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use hashbrown::HashMap;
use tokio::sync::RwLock;

use crate::{
    database::{DatabaseString, ListBlocker},
    id::Id,
};

pub type DB<T> = Arc<RwLock<hashbrown::HashMap<Bytes, T>>>;

pub type ListBlocklist = Arc<tokio::sync::Mutex<HashMap<Bytes, Vec<ListBlocker>>>>;

#[derive(Default)]
pub struct RedisDatabase {
    pub(crate) strings: DB<DatabaseString>,
    // pub(crate) nums: DB<i32>,
    pub(crate) streams: DB<Vec<DatabaseStreamEntry>>,
    pub(crate) lists: DB<VecDeque<Bytes>>,
    pub(crate) list_blocklist: ListBlocklist,
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatabaseStreamEntry {
    pub id: Id,
    pub values: HashMap<Bytes, Bytes>,
}

impl Ord for DatabaseStreamEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for DatabaseStreamEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
