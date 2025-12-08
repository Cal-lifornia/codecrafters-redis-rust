use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use hashbrown::HashMap;
use tokio::sync::RwLock;

use crate::{database::DatabaseString, id::Id};

pub type DB<T> = Arc<RwLock<hashbrown::HashMap<Bytes, T>>>;

#[derive(Default)]
pub struct RedisDatabase {
    pub(crate) strings: DB<DatabaseString>,
    pub(crate) nums: DB<i32>,
    pub(crate) streams: DB<Vec<DatabaseStreamEntry>>,
    pub(crate) lists: DB<VecDeque<Bytes>>,
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
