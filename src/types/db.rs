use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::types::EntryId;

pub type ListBlocklist = Arc<tokio::sync::Mutex<HashMap<Bytes, Vec<oneshot::Sender<Bytes>>>>>;
pub type StreamBlocklist = Arc<
    tokio::sync::Mutex<
        HashMap<Bytes, Vec<oneshot::Sender<Vec<(Bytes, Vec<DatabaseStreamEntry>)>>>>,
    >,
>;

#[derive(Debug, Clone)]
pub enum DatabaseEntry {
    String(DatabaseString),
    Integer(i32),
    List(VecDeque<Bytes>),
    Stream(Vec<DatabaseStreamEntry>),
}

#[derive(Debug, Default, Clone)]
pub struct DatabaseString {
    value: Bytes,
    expiry: Option<Instant>,
}
impl DatabaseString {
    pub fn new(value: Bytes, expiry: Option<Instant>) -> Self {
        Self { value, expiry }
    }
    pub fn value(&self) -> &Bytes {
        &self.value
    }
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            Instant::now() > expiry
        } else {
            false
        }
    }
    pub fn update_value_ttl_expiry(&mut self, value: Bytes) {
        self.value = value
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatabaseStreamEntry {
    pub id: EntryId,
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
