use std::{
    collections::HashMap,
    sync::{PoisonError, RwLock},
    time::{Duration, Instant},
};

use thiserror::Error;

pub struct RedisDatabase(RwLock<HashMap<String, DatabaseEntry>>);

impl RedisDatabase {
    pub fn init() -> Self {
        Self::default()
    }
}

impl Default for RedisDatabase {
    fn default() -> Self {
        Self(RwLock::new(HashMap::new()))
    }
}

impl RedisDatabase {
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

    pub fn push_list(&self, key: &str, values: &[String]) -> Result<i32, DatabaseError> {
        let mut db = self.0.write()?;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            list.extend_from_slice(values);
            Ok(list.len() as i32)
        } else {
            db.insert(key.to_string(), DatabaseEntry::List(values.to_vec()));
            Ok(values.len() as i32)
        }
    }

    pub fn prepend_list(&self, key: &str, values: Vec<String>) -> Result<i32, DatabaseError> {
        let mut db = self.0.write()?;
        let mut reversed = values.clone();
        reversed.reverse();
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            *list = [reversed, list.to_vec()].concat();
            Ok(list.len() as i32)
        } else {
            db.insert(key.to_string(), DatabaseEntry::List(reversed));
            Ok(values.len() as i32)
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

            Ok(list[start..=end].to_vec())
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

    pub fn pop_first_list(
        &self,
        key: &str,
        count: Option<usize>,
    ) -> Result<Option<Vec<String>>, DatabaseError> {
        let mut db = self.0.write()?;
        if let Some(DatabaseEntry::List(list)) = db.get_mut(key) {
            if let Some(count) = count {
                let mut result = list.split_off(count);
                std::mem::swap(&mut result, list);
                Ok(Some(result))
            } else {
                Ok(Some([list.remove(0)].to_vec()))
            }
        } else {
            Ok(None)
        }
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
}

#[derive(Debug, Clone)]
pub enum DatabaseEntry {
    String(DatabaseString),
    List(Vec<String>),
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
    PoisonError,
}

impl<T> From<PoisonError<T>> for DatabaseError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonError
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
        let db = RedisDatabase::init();
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
        let db = RedisDatabase::init();
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
            db.prepend_list("test", entry.to_vec()).unwrap();

            let result = db.read_list("test", 0, -1).unwrap();

            assert_eq!(result, expected)
        }
    }
    #[test]
    fn test_get_list_length() {
        let db = RedisDatabase::init();

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
        let db = RedisDatabase::init();

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
