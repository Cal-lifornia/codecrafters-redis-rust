use std::{
    collections::{HashMap, VecDeque},
    io::Read,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Buf, Bytes};
use tokio::{
    sync::{oneshot, Mutex, RwLock},
    task::JoinSet,
};

use crate::types::DatabaseStreamEntry;
use crate::types::{DatabaseEntry, DatabaseString, EntryId, ListBlocklist, StreamBlocklist};

pub type Database = Arc<RwLock<HashMap<Bytes, DatabaseEntry>>>;

pub struct RedisDatabase {
    db: Database,
    pub list_blocklist: ListBlocklist,
    pub stream_blocklist: StreamBlocklist,
}

impl Default for RedisDatabase {
    fn default() -> Self {
        Self {
            db: Arc::new(RwLock::new(HashMap::new())),
            list_blocklist: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            stream_blocklist: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl RedisDatabase {
    // async fn delete_expired_entries(&self) -> () {
    //     let mut db = self.db.write().await;
    //     let keys = self.get_expired_entries().await;
    //     for key in keys {
    //         db.remove(&key).unwrap();
    //     }
    //     Ok(())
    // }
    // async fn get_expired_entries(&self) -> Vec<String> {
    //     let db = self.db.read().await;
    //     Ok(db
    //         .iter()
    //         .filter(|(_, val)| {
    //             if let DatabaseEntry::String(entry) = val {
    //                 entry.is_expired()
    //             } else {
    //                 false
    //             }
    //         })
    //         .map(|(key, _)| key.clone())
    //         .collect())
    // }
    pub async fn set_key_value(
        &self,
        key: &Bytes,
        value: Bytes,
        expires: Option<Duration>,
        keep_ttl: bool,
    ) {
        let mut db = self.db.write().await;
        let expiry = expires.map(|e| Instant::now() + e);

        let mut buf = String::new();

        value.clone().reader().read_to_string(&mut buf).unwrap();

        match buf.parse::<i32>() {
            Ok(d_int) => {
                db.insert(key.clone(), DatabaseEntry::Integer(d_int));
            }
            Err(_) => {
                if keep_ttl {
                    if let Some(DatabaseEntry::String(entry)) = db.get_mut(key) {
                        if !entry.is_expired() {
                            entry.update_value_ttl_expiry(value.clone());
                        }
                    }
                }

                db.insert(
                    key.clone(),
                    DatabaseEntry::String(DatabaseString::new(value, expiry)),
                );
            }
        }
    }

    pub async fn get_key_value(&self, key: &Bytes) -> Option<Bytes> {
        let expired = {
            let db = self.db.read().await;
            match db.get(key) {
                Some(DatabaseEntry::String(value)) => {
                    if !value.is_expired() {
                        return Some(value.value().clone());
                    } else {
                        true
                    }
                }
                Some(DatabaseEntry::Integer(value)) => return Some(value.to_string().into()),
                Some(_) | None => return None,
            }
        };
        if expired {
            let mut db = self.db.write().await;
            db.remove(key);
        }
        None
    }

    pub async fn increase_integer(&self, key: &Bytes) -> Result<i32, std::io::Error> {
        let mut db = self.db.write().await;
        match db.get_mut(key) {
            Some(DatabaseEntry::Integer(value)) => {
                *value += 1;
                Ok(*value)
            }
            Some(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "value is not an integer or out of range",
            )),
            None => {
                db.insert(key.clone(), DatabaseEntry::Integer(1));
                Ok(1)
            }
        }
    }

    pub async fn push_list(&self, key: &Bytes, values: &[Bytes]) -> i32 {
        let mut db = self.db.write().await;
        let results = if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            list.extend(values.to_vec());
            list.len() as i32
        } else {
            db.insert(
                key.clone(),
                DatabaseEntry::List(VecDeque::from_iter(values.to_vec())),
            );
            values.len() as i32
        };
        drop(db);
        self.handle_blockers(key).await;
        results
    }

    pub async fn prepend_list(&self, key: &Bytes, values: &[Bytes]) -> i32 {
        let mut db = self.db.write().await;
        let results = if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            values
                .iter()
                .for_each(|value| list.push_front(value.clone()));
            list.len() as i32
        } else {
            db.insert(
                key.clone(),
                DatabaseEntry::List(VecDeque::from_iter(values.iter().rev().cloned())),
            );
            values.len() as i32
        };
        drop(db);
        self.handle_blockers(key).await;
        results
    }

    pub async fn read_list(&self, key: &Bytes, start: i32, end: i32) -> Vec<Bytes> {
        let db = self.db.read().await;
        if let Some(DatabaseEntry::List(list)) = db.get(key) {
            let len = list.len() as i32;
            if start > len - 1 {
                return [].to_vec();
            };
            let start = if start < 0 { len + start } else { start };
            let end = if end < 0 { len + end } else { end };
            let start = start.max(0) as usize;
            let end = end.min(len - 1) as usize;

            if start > end {
                return [].to_vec();
            }

            list.range(start..=end).cloned().collect()
        } else {
            [].to_vec()
        }
    }

    pub async fn get_list_length(&self, key: &Bytes) -> i32 {
        let db = self.db.read().await;
        if let Some(DatabaseEntry::List(list)) = db.get(key) {
            list.len() as i32
        } else {
            0
        }
    }

    pub async fn pop_front_list(&self, key: &Bytes, count: Option<usize>) -> Option<Vec<Bytes>> {
        let mut db = self.db.write().await;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            if let Some(mut count) = count {
                count = count.min(list.len());
                let mut results: Vec<Bytes> = vec![];
                for _ in 0..count {
                    results.push(list.pop_front().unwrap());
                }
                Some(results)
            } else {
                Some(vec![list.pop_front().unwrap()])
            }
        } else {
            None
        }
    }

    pub async fn check_pop_list(&self, key: &Bytes) -> Option<Bytes> {
        let mut db = self.db.write().await;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            if !list.is_empty() {
                return list.pop_front();
            }
        }
        None
    }
    pub async fn handle_blockers(&self, key: &Bytes) {
        let mut blockers = self.list_blocklist.lock().await;
        if let Some(waiters) = blockers.get_mut(key) {
            if !waiters.is_empty() {
                if let Some(sender) = get_open_sender(waiters) {
                    let mut db = self.db.write().await;
                    if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
                        let result = list.pop_front().expect("expected list item");
                        sender.send(result).unwrap();
                    }
                }
            }
            if waiters.is_empty() {
                blockers.remove(key);
            }
        }
    }

    pub async fn get_type(&self, key: &Bytes) -> String {
        let db = self.db.read().await;
        match db.get(key) {
            Some(DatabaseEntry::String(_)) => "string".into(),
            Some(DatabaseEntry::List(_)) => "list".into(),
            Some(DatabaseEntry::Stream(_)) => "stream".into(),
            Some(DatabaseEntry::Integer(_)) => "integer".into(),
            None => "none".into(),
        }
    }

    pub async fn add_stream(
        &self,
        key: &Bytes,
        id: EntryId,
        wildcard: bool,
        values: HashMap<Bytes, Bytes>,
    ) -> Option<String> {
        let mut db = self.db.write().await;
        let results = if let Some(DatabaseEntry::Stream(stream)) = db.get_mut(key) {
            let mut entry_id = id;
            if wildcard {
                if let Some(same_time) = stream.last() {
                    if same_time.id.ms_time == entry_id.ms_time {
                        entry_id.sequence = same_time.id.sequence + 1;
                    } else {
                        entry_id.sequence = if entry_id.ms_time == 0 { 1 } else { 0 }
                    }
                } else {
                    entry_id.sequence = if entry_id.ms_time == 0 { 1 } else { 0 }
                }
            }
            let max = stream.last().unwrap();
            if max.id >= entry_id {
                return None;
            }
            stream.push(DatabaseStreamEntry {
                id: entry_id,
                values: values.clone(),
            });
            stream.sort();
            Some(entry_id.to_string())
        } else {
            db.insert(
                key.clone(),
                DatabaseEntry::Stream(vec![DatabaseStreamEntry {
                    id,
                    values: values.clone(),
                }]),
            );
            Some(id.to_string())
        };
        drop(db);
        {
            let mut blockers = self.stream_blocklist.lock().await;
            let tasks = if let Some(waiters) = blockers.remove(key) {
                let mut tasks = JoinSet::new();
                for waiter in waiters {
                    let key = key.clone();
                    let values = values.clone();
                    tasks.spawn(async move {
                        waiter.send(vec![(key, vec![DatabaseStreamEntry { id, values }])])
                    });
                }
                Some(tasks)
            } else {
                None
            };
            drop(blockers);

            if let Some(mut tasks) = tasks {
                if let Some(Err(err)) = tasks.join_next().await {
                    eprintln!("ran into error {err}");
                }
            }
        }
        results
    }

    pub async fn range_stream(
        &self,
        key: &Bytes,
        start: Option<EntryId>,
        stop: Option<EntryId>,
    ) -> Vec<DatabaseStreamEntry> {
        let db = self.db.read().await;
        if let Some(DatabaseEntry::Stream(stream)) = db.get(key) {
            let first_point = match start {
                Some(start) => stream.partition_point(|value| value.id < start),
                None => 0,
            };
            if let Some(stop) = stop {
                let last_point = stream.partition_point(|value| value.id <= stop);
                stream[first_point..last_point].to_vec()
            } else {
                stream[first_point..].to_vec()
            }
        } else {
            vec![]
        }
    }

    pub async fn read_stream(
        &self,
        keys: &[Bytes],
        ids: &[EntryId],
    ) -> Vec<(Bytes, Vec<DatabaseStreamEntry>)> {
        let db = self.db.read().await;
        let mut results: Vec<(Bytes, Vec<DatabaseStreamEntry>)> = vec![];
        keys.iter().zip(ids.iter()).for_each(|(key, id)| {
            if let Some(DatabaseEntry::Stream(stream)) = db.get(key) {
                let result = stream.partition_point(|value| value.id < *id);
                results.push((key.clone(), stream[result..].to_vec()));
            }
        });
        results
    }
}

fn get_open_sender(waiters: &mut Vec<oneshot::Sender<Bytes>>) -> Option<oneshot::Sender<Bytes>> {
    for _ in 0..waiters.len() {
        let waiter = waiters.remove(0);
        if !waiter.is_closed() {
            return Some(waiter);
        }
    }
    None
}

// #[cfg(test)]
// mod tests {
//     use std::thread::sleep;

//     use super::*;

//     #[test]
//     fn test_is_expired() {
//         let test_entry =
//             DatabaseString::new("test", Some(Instant::now() + Duration::from_millis(100)));
//         sleep(Duration::from_millis(105));

//         assert!(test_entry.is_expired())
//     }

//     #[test]
//     fn test_read_list() {
//         let db = Database::default();
//         let test_entry = vec![
//             "pear".to_string(),
//             "apple".to_string(),
//             "banana".to_string(),
//             "orange".to_string(),
//             "blueberry".to_string(),
//             "strawberry".to_string(),
//             "raspberry".to_string(),
//         ];

//         db.push_list("test", &test_entry).unwrap();

//         let test_input = [(0, 1), (0, -6), (-9, -6)];
//         let expected = vec!["pear".to_string(), "apple".to_string()];

//         for (i, (start, end)) in test_input.iter().enumerate() {
//             let results = db.read_list("test", *start, *end).unwrap();
//             assert_eq!(results, expected, "testing case {i}")
//         }
//     }

//     #[test]
//     fn test_prepend_list() {
//         let db = Database::default();
//         let mut test_entries = [
//             vec!["a".to_string(), "b".to_string(), "c".to_string()],
//             vec!["d".to_string()],
//         ];

//         let expecting = vec![
//             vec!["c".to_string(), "b".to_string(), "a".to_string()],
//             vec![
//                 "d".to_string(),
//                 "c".to_string(),
//                 "b".to_string(),
//                 "a".to_string(),
//             ],
//         ];

//         for (entry, expected) in test_entries.iter_mut().zip(expecting) {
//             db.prepend_list("test", entry).unwrap();

//             let result = db.read_list("test", 0, -1).unwrap();

//             assert_eq!(result, expected)
//         }
//     }
//     #[test]
//     fn test_get_list_length() {
//         let db = Database::default();

//         let test_entries = [
//             vec!["a".to_string(), "b".to_string(), "c".to_string()],
//             vec!["d".to_string()],
//         ];

//         let expecting = [3, 1, 0];

//         for (i, (entry, expected)) in test_entries.iter().zip(expecting).enumerate() {
//             db.push_list(format!("test{i}").as_str(), entry).unwrap();

//             let result = db.get_list_length(format!("test{i}").as_str()).unwrap();
//             assert_eq!(result, expected)
//         }
//     }

//     #[test]
//     fn test_pop_first_list() {
//         let db = Database::default();

//         let test_entry = [
//             "a".to_string(),
//             "b".to_string(),
//             "c".to_string(),
//             "d".to_string(),
//         ];

//         let test_count = [None, Some(2usize)];

//         let expecting = [
//             vec!["a".to_string()],
//             vec!["b".to_string(), "c".to_string()],
//         ];

//         db.push_list("test", &test_entry).unwrap();

//         for (expected, count) in expecting.iter().zip(test_count) {
//             let result = db.pop_first_list("test", count).unwrap();
//             assert_eq!(result, Some(expected.clone()))
//         }
//     }
// }
