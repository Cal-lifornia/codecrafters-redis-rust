use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use either::Either;
use hashbrown::HashMap;
use indexmap::IndexMap;
use tokio::{
    sync::{RwLock, mpsc, oneshot},
    time::Instant,
};

use crate::{
    ArcLock, Pair,
    command::macros::Symbol,
    database::{BlpopResponse, DatabaseValue, Location, ReadStreamResult, channels::ChannelDB},
    id::Id,
    rdb::RdbKeyValue,
};

pub type DB<T> = Arc<RwLock<hashbrown::HashMap<Bytes, T>>>;

pub type Blocklist<T> = Arc<tokio::sync::Mutex<HashMap<Bytes, T>>>;

pub struct Blocker<T> {
    pub sender: T,
    pub timeout: Option<Instant>,
}

impl<T> Blocker<T> {
    pub fn timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            Instant::now() > timeout
        } else {
            false
        }
    }
}

type StreamBlocklist =
    Blocklist<Vec<Pair<Either<Id, Symbol!("$")>, Blocker<mpsc::Sender<ReadStreamResult>>>>>;

#[derive(Default)]
pub struct RedisDatabase {
    pub(crate) key_value: DB<DatabaseValue>,
    pub(crate) streams: DB<IndexMap<Id, HashMap<Bytes, Bytes>>>,
    pub(crate) lists: DB<VecDeque<Bytes>>,
    pub(crate) sets: DB<IndexMap<Bytes, f64>>,
    pub(crate) geo: DB<IndexMap<Bytes, Location>>,
    pub(crate) channels: ArcLock<ChannelDB>,
    pub(crate) list_blocklist: Blocklist<Vec<Blocker<oneshot::Sender<BlpopResponse>>>>,
    pub(crate) stream_blocklist: StreamBlocklist,
}

impl RedisDatabase {
    pub async fn db_type(&self, key: &Bytes) -> Bytes {
        let value = if self.key_value.read().await.contains_key(key) {
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
    pub async fn keys(&self, filter: &Bytes) -> Vec<Bytes> {
        let key_value = self.key_value.read().await;
        let stream = self.streams.read().await;
        let list = self.lists.read().await;
        let kv_keys = key_value.keys();
        let stream_keys = stream.keys();
        let list_keys = list.keys();
        let all_keys = kv_keys.chain(stream_keys).chain(list_keys);
        if filter.to_ascii_lowercase().as_slice() == b"*" {
            let mut out: Vec<Bytes> = all_keys.cloned().collect();
            out.sort();
            out
        } else {
            let mut out: Vec<Bytes> = all_keys
                .filter(|key| key.starts_with(filter))
                .cloned()
                .collect();
            out.sort();
            out
        }
    }
    pub async fn from_rdb(keys: impl IntoIterator<Item = RdbKeyValue>) -> Self {
        let db = RedisDatabase::default();
        for kv in keys.into_iter() {
            db.set_kv(
                kv.key().clone(),
                kv.value().clone(),
                kv.expiry().map(Either::Right),
                false,
            )
            .await;
        }
        db
    }
}
