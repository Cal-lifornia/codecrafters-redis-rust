use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use crate::{commands::Responder, types::EntryId};

pub type ListBlocklist =
    Arc<tokio::sync::Mutex<HashMap<String, Vec<Responder<Option<Vec<String>>>>>>>;
pub type StreamBlocklist = Arc<
    tokio::sync::Mutex<HashMap<String, Vec<Responder<Vec<(String, Vec<DatabaseStreamEntry>)>>>>>,
>;

#[derive(Debug, Clone)]
pub enum DatabaseEntry {
    String(DatabaseString),
    List(VecDeque<String>),
    Stream(Vec<DatabaseStreamEntry>),
}

#[derive(Debug, Default, Clone)]
pub struct DatabaseString {
    value: String,
    expiry: Option<Instant>,
}
impl DatabaseString {
    pub fn new(value: &str, expiry: Option<Instant>) -> Self {
        Self {
            value: value.to_string(),
            expiry,
        }
    }
    pub fn value(&self) -> &str {
        &self.value
    }
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            Instant::now() > expiry
        } else {
            false
        }
    }
    pub fn update_value_ttl_expiry(&mut self, value: &str) {
        self.value = value.to_string()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatabaseStreamEntry {
    pub id: EntryId,
    pub values: HashMap<String, String>,
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
