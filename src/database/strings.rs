use bytes::Bytes;
use tokio::time::Instant;

use crate::database::RedisDatabase;

#[derive(Debug, Default, Clone)]
pub struct DatabaseString {
    value: Bytes,
    expiry: Option<Instant>,
}

impl DatabaseString {
    // pub fn new(value: Bytes, expiry: Option<Instant>) -> Self {
    //     Self { value, expiry }
    // }
    // pub fn value(&self) -> &Bytes {
    //     &self.value
    // }
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            Instant::now() > expiry
        } else {
            false
        }
    }
    // pub fn update_value_ttl_expiry(&mut self, value: Bytes) {
    //     self.value = value
    // }
}

impl RedisDatabase {
    pub async fn set_string(
        &self,
        key: Bytes,
        val: Bytes,
        expiry: Option<Instant>,
        _keep_ttl: bool,
    ) {
        let mut string_db = self.strings.write().await;
        string_db.insert(key, DatabaseString { value: val, expiry });
    }
    pub async fn get_string(&self, key: &Bytes) -> Option<Bytes> {
        let string_db = self.strings.read().await;
        if let Some(val) = string_db.get(key) {
            if val.is_expired() {
                None
            } else {
                Some(val.value.clone())
            }
        } else {
            None
        }
    }
}
