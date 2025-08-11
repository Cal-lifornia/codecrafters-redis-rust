use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::{
    commands::{RedisCommand, Responder},
    types::EntryId,
};

mod types;
pub use types::*;

struct Database(RwLock<HashMap<String, DatabaseEntry>>);

impl Default for Database {
    fn default() -> Self {
        Self(RwLock::new(HashMap::new()))
    }
}

pub struct RedisDatabase {
    db: Database,
    sender: mpsc::Sender<RedisCommand>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<RedisCommand>>,
    blocklist: DbBlocklist,
}

impl Default for RedisDatabase {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let receiver = tokio::sync::Mutex::new(receiver);
        Self {
            db: Database::default(),
            sender,
            receiver,
            blocklist: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }
}

impl RedisDatabase {
    pub fn clone_sender(&self) -> mpsc::Sender<RedisCommand> {
        self.sender.clone()
    }

    pub async fn handle_receiver(&self) -> Result<(), DatabaseError> {
        while let Some(command) = self.receiver.lock().await.recv().await {
            use RedisCommand::*;

            match command {
                Get {
                    key,
                    responder: resp,
                } => resp.send(self.db.get_string(&key).await).unwrap(),

                Set { args, responder } => responder
                    .send(
                        self.db
                            .set_string(&args.key, &args.value, args.expiry, args.keep_ttl)
                            .await,
                    )
                    .unwrap(),
                Rpush {
                    key,
                    values,
                    responder,
                } => {
                    responder
                        .send(self.db.push_list(&key, &values).await)
                        .unwrap();
                    let blockers = Arc::clone(&self.blocklist);
                    self.db
                        .handle_blockers(key.as_str(), &blockers.clone())
                        .await?;
                }
                Lpush {
                    key,
                    values,
                    responder,
                } => {
                    responder
                        .send(self.db.prepend_list(&key, &values).await)
                        .unwrap();
                    let blockers = Arc::clone(&self.blocklist);
                    self.db
                        .handle_blockers(key.as_str(), &blockers.clone())
                        .await?;
                }
                Lrange {
                    key,
                    start,
                    end,
                    responder,
                } => responder
                    .send(self.db.read_list(&key, start, end).await)
                    .unwrap(),
                Llen { key, responder } => {
                    responder.send(self.db.get_list_length(&key).await).unwrap()
                }
                Lpop {
                    key,
                    count,
                    responder,
                } => responder
                    .send(self.db.pop_first_list(&key, count).await)
                    .unwrap(),
                Blpop { key, responder } => {
                    match self.db.blocking_pop_first_list(key.as_str()).await {
                        Ok(Some(result)) => {
                            responder.send(Ok(Some(result))).unwrap();
                        }
                        Ok(None) => {
                            let mut blockers = self.blocklist.lock().await;
                            if let Some(list) = blockers.get_mut(&key) {
                                list.push(responder);
                            } else {
                                blockers.insert(key, vec![responder]);
                            }
                        }
                        Err(err) => responder.send(Err(err)).unwrap(),
                    }
                }
                Type { key, responder } => {
                    responder.send(self.db.get_type(&key).await).unwrap();
                }
                Xadd {
                    key,
                    id,
                    values,
                    wildcard,
                    responder,
                } => {
                    responder
                        .send(self.db.add_stream(&key, &id, wildcard, values).await)
                        .unwrap();
                }
                Xrange {
                    key,
                    start,
                    stop,
                    responder,
                } => {
                    responder
                        .send(self.db.range_stream(&key, start, stop).await)
                        .unwrap();
                }
            }
        }
        Ok(())
    }
}

impl Database {
    // async fn delete_expired_entries(&self) -> Result<(), DatabaseError> {
    //     let mut db = self.0.write().await;
    //     let keys = self.get_expired_entries().await;
    //     for key in keys {
    //         db.remove(&key).unwrap();
    //     }
    //     Ok(())
    // }
    // async fn get_expired_entries(&self) -> Result<Vec<String>, DatabaseError> {
    //     let db = self.0.read().await;
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
    pub async fn set_string(
        &self,
        key: &str,
        value: &str,
        expires: Option<Duration>,
        keep_ttl: bool,
    ) -> Result<(), DatabaseError> {
        let mut db = self.0.write().await;
        let expiry = expires.map(|e| Instant::now() + e);

        if keep_ttl {
            if let Some(DatabaseEntry::String(entry)) = db.get_mut(key) {
                if !entry.is_expired() {
                    entry.update_value_ttl_expiry(value);
                    return Ok(());
                }
            }
        }

        db.insert(
            key.to_string(),
            DatabaseEntry::String(DatabaseString::new(value, expiry)),
        );
        Ok(())
    }

    pub async fn get_string(&self, key: &str) -> Result<Option<String>, DatabaseError> {
        let expired = {
            let db = self.0.read().await;
            if let Some(DatabaseEntry::String(entry)) = db.get(key) {
                if !entry.is_expired() {
                    return Ok(Some(entry.value().to_string()));
                } else {
                    true
                }
            } else {
                return Ok(None);
            }
        };

        if expired {
            let mut db = self.0.write().await;
            db.remove(key);
        }
        Ok(None)
    }

    pub async fn push_list(&self, key: &str, values: &[String]) -> Result<i32, DatabaseError> {
        let mut db = self.0.write().await;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            list.extend(
                values
                    .iter()
                    .map(|val| val.to_string())
                    .collect::<VecDeque<String>>(),
            );
            Ok(list.len() as i32)
        } else {
            db.insert(
                key.to_string(),
                DatabaseEntry::List(values.iter().map(|val| val.to_string()).collect()),
            );
            Ok(values.len() as i32)
        }
    }

    pub async fn prepend_list(&self, key: &str, values: &[String]) -> Result<i32, DatabaseError> {
        let mut db = self.0.write().await;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            values
                .iter()
                .for_each(|value| list.push_front(value.to_string()));
            Ok(list.len() as i32)
        } else {
            db.insert(
                key.to_string(),
                DatabaseEntry::List(values.iter().rev().map(|value| value.to_string()).collect()),
            );
            Ok(values.len() as i32)
        }
    }

    pub async fn read_list(
        &self,
        key: &str,
        start: i32,
        end: i32,
    ) -> Result<Vec<String>, DatabaseError> {
        let db = self.0.read().await;
        if let Some(DatabaseEntry::List(list)) = db.get(key) {
            let len = list.len() as i32;
            if start > len - 1 {
                return Ok([].to_vec());
            };
            let start = if start < 0 { len + start } else { start };
            let end = if end < 0 { len + end } else { end };
            let start = start.max(0) as usize;
            let end = end.min(len - 1) as usize;

            if start > end {
                return Ok([].to_vec());
            }

            Ok(list.range(start..=end).cloned().collect())
        } else {
            Ok([].to_vec())
        }
    }

    pub async fn get_list_length(&self, key: &str) -> Result<i32, DatabaseError> {
        let db = self.0.read().await;
        if let Some(DatabaseEntry::List(list)) = db.get(key) {
            Ok(list.len() as i32)
        } else {
            Ok(0)
        }
    }

    async fn pop_front_list_only(&self, key: &str) -> Result<Option<String>, DatabaseError> {
        let mut db = self.0.write().await;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            let result = list.pop_front();
            Ok(result)
        } else {
            Ok(None)
        }
    }

    pub async fn pop_first_list(
        &self,
        key: &str,
        count: Option<usize>,
    ) -> Result<Option<Vec<String>>, DatabaseError> {
        let mut db = self.0.write().await;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            if let Some(mut count) = count {
                count = count.min(list.len());
                let mut results: Vec<String> = vec![];
                for _ in 0..count {
                    results.push(list.pop_front().unwrap());
                }
                Ok(Some(results))
            } else {
                Ok(Some(vec![list.pop_front().unwrap()]))
            }
        } else {
            Ok(None)
        }
    }

    pub async fn blocking_pop_first_list(
        &self,
        key: &str,
    ) -> Result<Option<Vec<String>>, DatabaseError> {
        let db = self.0.read().await;
        if let Some(DatabaseEntry::List(list)) = db.get(key) {
            if !list.is_empty() {
                if let Some(result) = self.pop_front_list_only(key).await? {
                    return Ok(Some(vec![key.to_string(), result]));
                }
            }
        }
        Ok(None)
    }
    async fn handle_blockers(
        &self,
        key: &str,
        blocklist: &DbBlocklist,
    ) -> Result<(), DatabaseError> {
        let mut blockers = blocklist.lock().await;
        if let Some(waiters) = blockers.get_mut(key) {
            if !waiters.is_empty() {
                if let Some(sender) = get_open_sender(waiters) {
                    match self.pop_front_list_only(key).await? {
                        Some(result) => {
                            sender
                                .send(Ok(Some(vec![key.to_string(), result])))
                                .unwrap();
                        }
                        None => {
                            sender.send(Ok(None)).unwrap();
                        }
                    }
                }
            }

            if waiters.is_empty() {
                blockers.remove(key);
            }
        }
        Ok(())
    }

    async fn get_type(&self, key: &str) -> Result<String, DatabaseError> {
        let db = self.0.read().await;
        match db.get(key) {
            Some(DatabaseEntry::String(_)) => Ok("string".into()),
            Some(DatabaseEntry::List(_)) => Ok("list".into()),
            Some(DatabaseEntry::Stream(_)) => Ok("stream".into()),
            None => Ok("none".into()),
        }
    }

    async fn add_stream(
        &self,
        key: &str,
        id: &EntryId,
        wildcard: bool,
        values: HashMap<String, String>,
    ) -> Result<Option<String>, DatabaseError> {
        let mut db = self.0.write().await;
        if let Some(DatabaseEntry::Stream(stream)) = db.get_mut(key) {
            let mut entry_id = *id;
            if wildcard {
                if let Some(same_time) = stream.last() {
                    entry_id.sequence = same_time.id.sequence + 1;
                } else {
                    entry_id.sequence = if entry_id.ms_time == 0 { 1 } else { 0 }
                }
            }
            let max = stream.last().unwrap();
            if max.id >= entry_id {
                return Ok(None);
            }
            stream.push(DatabaseStreamEntry {
                id: entry_id,
                values,
            });
            stream.sort();
            Ok(Some(entry_id.to_string()))
        } else {
            db.insert(
                key.to_string(),
                DatabaseEntry::Stream(vec![DatabaseStreamEntry { id: *id, values }]),
            );
            Ok(Some(id.to_string()))
        }
    }

    async fn range_stream(
        &self,
        key: &str,
        start: Option<EntryId>,
        stop: Option<EntryId>,
    ) -> Result<Vec<DatabaseStreamEntry>, DatabaseError> {
        let db = self.0.read().await;
        if let Some(DatabaseEntry::Stream(stream)) = db.get(key) {
            let first_point = match start {
                Some(start) => stream
                    .binary_search_by(|value| value.id.cmp(&start))
                    .unwrap(),
                None => 0,
            };
            if let Some(stop) = stop {
                let last_point = stream
                    .binary_search_by(|value| value.id.cmp(&stop))
                    .unwrap();
                Ok(stream[first_point..=last_point].to_vec())
            } else {
                Ok(stream[first_point..].to_vec())
            }
        } else {
            Ok(vec![])
        }
    }
}

fn get_open_sender(
    waiters: &mut Vec<Responder<Option<Vec<String>>>>,
) -> Option<Responder<Option<Vec<String>>>> {
    for _ in 0..waiters.len() {
        let sender = waiters.remove(0);
        if !sender.is_closed() {
            return Some(sender);
        }
    }
    None
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("channel failed to send")]
    ChannelSendError(String),
}

impl From<oneshot::error::RecvError> for DatabaseError {
    fn from(value: oneshot::error::RecvError) -> Self {
        Self::ChannelSendError(value.to_string())
    }
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
