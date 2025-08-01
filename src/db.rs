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
            Ok(1)
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
}
