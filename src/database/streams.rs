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
        key: &Bytes,
        id: WildcardID,
        values: HashMap<Bytes, Bytes>,
    ) -> Id {
        let mut streams = self.streams.write().await;
        if let Some(stream) = streams.get_mut(key) {
            if let Some(id) = Id::from_wildcard(id) {
                if let Some(last) = stream.last() {
                    if last.id < id {
                        stream.push(DatabaseStreamEntry { id, values });
                        id
                    } else {
                        todo!()
                    }
                } else {
                    stream.push(DatabaseStreamEntry { id, values });
                    id
                }
            } else {
                todo!()
            }
        } else if let Some(id) = Id::from_wildcard(id) {
            streams.insert(key.clone(), vec![DatabaseStreamEntry { id, values }]);
            id
        } else {
            todo!()
        }
    }
}
