use bytes::Bytes;
use hashbrown::HashMap;

use crate::{
    database::RedisDatabase,
    id::{Id, WildcardID},
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatabaseStreamEntry {
    pub id: Id,
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

impl RedisDatabase {
    pub async fn add_stream(
        &self,
        key: Bytes,
        id: WildcardID,
        values: HashMap<Bytes, Bytes>,
    ) -> Result<Id, DbStreamAddError> {
        let mut streams = self.streams.write().await;
        let stream = streams.entry(key).or_default();
        if let Some(id) = Id::from_wildcard(id) {
            if id.is_zero_zero() {
                return Err(DbStreamAddError::IdZeroZero);
            }
            if let Some(last) = stream.last() {
                if id > last.id {
                    stream.push(DatabaseStreamEntry { id, values });
                    Ok(id)
                } else {
                    Err(DbStreamAddError::IdNotGreater)
                }
            } else {
                stream.push(DatabaseStreamEntry { id, values });
                Ok(id)
            }
        } else if let Some(ms_time) = id.ms_time {
            if let Some(last) = stream.last()
                && last.id.ms_time == ms_time
            {
                let id = last.id.increment_sequence();
                stream.push(DatabaseStreamEntry { id, values });
                Ok(id)
            } else {
                let sequence = if ms_time == 0 { 1 } else { 0 };
                let id = Id { ms_time, sequence };
                stream.push(DatabaseStreamEntry { id, values });
                Ok(id)
            }
        } else {
            todo!()
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DbStreamAddError {
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    IdNotGreater,
    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    IdZeroZero,
}
