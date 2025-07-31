use std::{
    collections::HashMap,
    sync::{LazyLock, PoisonError, RwLock},
};

use thiserror::Error;

pub struct RedisDatabase(RwLock<HashMap<String, String>>);

impl RedisDatabase {
    pub fn set(&self, key: &str, value: &str) -> Result<(), DatabaseError> {
        let mut db = self.0.write()?;
        db.insert(key.to_string(), value.to_string());
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, DatabaseError> {
        let db = self.0.read()?;
        Ok(db.get(key).cloned())
    }
}

impl Default for RedisDatabase {
    fn default() -> Self {
        Self(RwLock::new(HashMap::new()))
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
