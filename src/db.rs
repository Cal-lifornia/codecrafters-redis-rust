use std::collections::HashMap;

use anyhow::Result;
use thiserror::Error;

pub struct DB {
    db: HashMap<Box<String>, Box<String>>,
}

impl DB {
    pub fn new() -> Self {
        Self {
            db: HashMap::default(),
        }
    }
    pub fn set(&mut self, key: String, value: String) {
        self.db.insert(Box::new(key), Box::new(value)).unwrap();
    }

    pub fn get(&self, key: String) -> Result<String, DBError> {
        match self.db.get(&Box::new(key)) {
            Some(val) => Ok(val.to_string()),
            None => Err(DBError::ValueDoesNotExist),
        }
    }
}

#[derive(Debug, Error)]
pub enum DBError {
    #[error("value already exists in db")]
    ValueDoesNotExist,
}
