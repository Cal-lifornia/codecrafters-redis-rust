use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, PoisonError, RwLock},
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::commands::{RedisCommand, Responder};

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

type DbBlocklist = Arc<tokio::sync::Mutex<HashMap<String, Vec<Responder<Option<String>>>>>>;

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
                } => resp.send(self.db.get_string(&key)).unwrap(),

                Set { args, responder } => responder
                    .send(
                        self.db
                            .set_string(&args.key, &args.value, args.expiry, args.keep_ttl),
                    )
                    .unwrap(),
                Rpush {
                    key,
                    values,
                    responder,
                } => {
                    responder.send(self.db.push_list(&key, &values)).unwrap();
                    let blockers = Arc::clone(&self.blocklist);
                    self.db.handle_blockers(key.as_str(), blockers).await?;
                }
                Lpush {
                    key,
                    values,
                    responder,
                } => {
                    responder.send(self.db.prepend_list(&key, &values)).unwrap();
                    let blockers = Arc::clone(&self.blocklist);
                    self.db.handle_blockers(key.as_str(), blockers).await?;
                }
                Lrange {
                    key,
                    start,
                    end,
                    responder,
                } => responder.send(self.db.read_list(&key, start, end)).unwrap(),
                Llen { key, responder } => responder.send(self.db.get_list_length(&key)).unwrap(),
                Lpop {
                    key,
                    count,
                    responder,
                } => responder.send(self.db.pop_first_list(&key, count)).unwrap(),
                Blpop {
                    key,
                    count,
                    responder,
                } => {
                    if let Ok(Some(result)) = self.db.blocking_pop_first_list(key.as_str(), count) {
                        responder.send(Ok(Some(result))).unwrap();
                        return Ok(());
                    }
                    let mut blockers = self.blocklist.lock().await;
                    blockers.entry(key.clone()).or_default().push(responder);
                }
            }
        }
        Ok(())
    }
}

impl Database {
    fn delete_expired_entries(&self) -> Result<(), DatabaseError> {
        let mut db = self.0.write()?;
        let keys = self.get_expired_entries()?;
        for key in keys {
            db.remove(&key).unwrap();
        }
        Ok(())
    }
    fn get_expired_entries(&self) -> Result<Vec<String>, DatabaseError> {
        let db = self.0.read()?;
        Ok(db
            .iter()
            .filter(|(_, val)| {
                if let DatabaseEntry::String(entry) = val {
                    entry.is_expired()
                } else {
                    false
                }
            })
            .map(|(key, _)| key.clone())
            .collect())
    }
    pub fn set_string(
        &self,
        key: &str,
        value: &str,
        expires: Option<Duration>,
        keep_ttl: bool,
    ) -> Result<(), DatabaseError> {
        let mut db = self.0.write()?;
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

    pub fn get_string(&self, key: &str) -> Result<Option<String>, DatabaseError> {
        let expired = {
            let db = self.0.read()?;
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
            let mut db = self.0.write()?;
            db.remove(key);
        }
        Ok(None)
    }

    pub fn push_list(&self, key: &str, values: &[String]) -> Result<i32, DatabaseError> {
        let mut db = self.0.write()?;
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

    pub fn prepend_list(&self, key: &str, values: &[String]) -> Result<i32, DatabaseError> {
        let mut db = self.0.write()?;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            values
                .iter()
                .for_each(|value| list.push_front(value.to_string()));
            Ok(list.len() as i32)
        } else {
            let mut list = VecDeque::new();
            values
                .iter()
                .for_each(|value| list.push_front(value.to_string()));
            let len = list.len();
            db.insert(key.to_string(), DatabaseEntry::List(list));
            Ok(len as i32)
        }
    }

    pub fn read_list(&self, key: &str, start: i32, end: i32) -> Result<Vec<String>, DatabaseError> {
        let db = self.0.read()?;
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

    pub fn get_list_length(&self, key: &str) -> Result<i32, DatabaseError> {
        let db = self.0.read()?;
        if let Some(DatabaseEntry::List(list)) = db.get(key) {
            Ok(list.len() as i32)
        } else {
            Ok(0)
        }
    }

    fn pop_front_list_only(&self, key: &str) -> Result<Option<String>, DatabaseError> {
        let mut db = self.0.write()?;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            let result = list.pop_front();
            Ok(result)
        } else {
            Ok(None)
        }
    }

    async fn handle_blockers(
        &self,
        key: &str,
        blocklist: DbBlocklist,
    ) -> Result<(), DatabaseError> {
        println!("we are here");
        let mut blockers = blocklist.lock().await;
        if let Some(waiters) = blockers.get_mut(key) {
            if !waiters.is_empty() {
                let sender = waiters.remove(0);
                sender.send(self.pop_front_list_only(key)).unwrap();
            }

            if waiters.is_empty() {
                blockers.remove(key);
            }
        }
        Ok(())
    }

    pub fn pop_first_list(
        &self,
        key: &str,
        count: Option<usize>,
    ) -> Result<Option<Vec<String>>, DatabaseError> {
        let mut db = self.0.write()?;
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

    pub fn blocking_pop_first_list(
        &self,
        key: &str,
        _timeout: usize,
    ) -> Result<Option<String>, DatabaseError> {
        let db = self.0.read()?;
        if let Some(DatabaseEntry::List(list)) = db.get(key) {
            if !list.is_empty() {
                if let Some(result) = self.pop_front_list_only(key)? {
                    return Ok(Some(result));
                };
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub enum DatabaseEntry {
    String(DatabaseString),
    List(VecDeque<String>),
}

#[derive(Debug, Default, Clone)]
pub struct DatabaseString {
    value: String,
    expiry: Option<Instant>,
}
impl DatabaseString {
    fn new(value: &str, expiry: Option<Instant>) -> Self {
        Self {
            value: value.to_string(),
            expiry,
        }
    }
    fn value(&self) -> &str {
        &self.value
    }
    fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            Instant::now() > expiry
        } else {
            false
        }
    }
    fn update_value_ttl_expiry(&mut self, value: &str) {
        self.value = value.to_string()
    }
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("database lock is poisoned")]
    LockPoisonError,
    #[error("channel failed to send")]
    ChannelSendError(String),
}

impl<T> From<PoisonError<T>> for DatabaseError {
    fn from(_: PoisonError<T>) -> Self {
        Self::LockPoisonError
    }
}

impl From<oneshot::error::RecvError> for DatabaseError {
    fn from(value: oneshot::error::RecvError) -> Self {
        Self::ChannelSendError(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use super::*;

    #[test]
    fn test_is_expired() {
        let test_entry =
            DatabaseString::new("test", Some(Instant::now() + Duration::from_millis(100)));
        sleep(Duration::from_millis(105));

        assert!(test_entry.is_expired())
    }

    #[test]
    fn test_read_list() {
        let db = Database::default();
        let test_entry = vec![
            "pear".to_string(),
            "apple".to_string(),
            "banana".to_string(),
            "orange".to_string(),
            "blueberry".to_string(),
            "strawberry".to_string(),
            "raspberry".to_string(),
        ];

        db.push_list("test", &test_entry).unwrap();

        let test_input = [(0, 1), (0, -6), (-9, -6)];
        let expected = vec!["pear".to_string(), "apple".to_string()];

        for (i, (start, end)) in test_input.iter().enumerate() {
            let results = db.read_list("test", *start, *end).unwrap();
            assert_eq!(results, expected, "testing case {i}")
        }
    }

    #[test]
    fn test_prepend_list() {
        let db = Database::default();
        let mut test_entries = [
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec!["d".to_string()],
        ];

        let expecting = vec![
            vec!["c".to_string(), "b".to_string(), "a".to_string()],
            vec![
                "d".to_string(),
                "c".to_string(),
                "b".to_string(),
                "a".to_string(),
            ],
        ];

        for (entry, expected) in test_entries.iter_mut().zip(expecting) {
            db.prepend_list("test", entry).unwrap();

            let result = db.read_list("test", 0, -1).unwrap();

            assert_eq!(result, expected)
        }
    }
    #[test]
    fn test_get_list_length() {
        let db = Database::default();

        let test_entries = [
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec!["d".to_string()],
        ];

        let expecting = [3, 1, 0];

        for (i, (entry, expected)) in test_entries.iter().zip(expecting).enumerate() {
            db.push_list(format!("test{i}").as_str(), entry).unwrap();

            let result = db.get_list_length(format!("test{i}").as_str()).unwrap();
            assert_eq!(result, expected)
        }
    }

    #[test]
    fn test_pop_first_list() {
        let db = Database::default();

        let test_entry = [
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];

        let test_count = [None, Some(2usize)];

        let expecting = [
            vec!["a".to_string()],
            vec!["b".to_string(), "c".to_string()],
        ];

        db.push_list("test", &test_entry).unwrap();

        for (expected, count) in expecting.iter().zip(test_count) {
            let result = db.pop_first_list("test", count).unwrap();
            assert_eq!(result, Some(expected.clone()))
        }
    }
}
