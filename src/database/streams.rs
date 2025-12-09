use std::time::SystemTime;

use bytes::{BufMut, Bytes};
use hashbrown::HashMap;

use crate::{
    command::XrangeIdInput,
    database::RedisDatabase,
    id::{Id, WildcardID},
    resp::RedisWrite,
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
impl RedisWrite for DatabaseStreamEntry {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut) {
        buf.put_slice(b"*2\r\n");
        self.id.write_to_buf(buf);
        self.values.write_to_buf(buf);
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
            if let Some((last_id, _)) = stream.last() {
                if &id > last_id {
                    stream.insert(id, values);
                    Ok(id)
                } else {
                    Err(DbStreamAddError::IdNotGreater)
                }
            } else {
                stream.insert(id, values);
                Ok(id)
            }
        } else {
            let ms_time = if let Some(ms_time) = id.ms_time {
                ms_time
            } else {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_millis() as usize
            };
            if let Some((last_id, _)) = stream.last()
                && last_id.ms_time == ms_time
            {
                let id = last_id.increment_sequence();
                stream.insert(id, values);
                Ok(id)
            } else {
                let sequence = if ms_time == 0 { 1 } else { 0 };
                let id = Id { ms_time, sequence };
                stream.insert(id, values);
                Ok(id)
            }
        }
    }
    pub async fn range_stream(
        &self,
        key: &Bytes,
        start: Option<&XrangeIdInput>,
        end: Option<&XrangeIdInput>,
    ) -> Vec<DatabaseStreamEntry> {
        let streams = self.streams.read().await;
        if let Some(stream) = streams.get(key) {
            if let Some(start) = start
                && let Some(stop) = end
            {
                let first = match start {
                    XrangeIdInput::MsTime(ms_time) => {
                        let id = Id {
                            ms_time: *ms_time as usize,
                            sequence: 0,
                        };
                        stream.partition_point(|key_id, _value| key_id < &id)
                    }
                    XrangeIdInput::Id(id) => {
                        if let Some(idx) = stream.get_index_of(id) {
                            idx
                        } else {
                            todo!()
                        }
                    }
                };
                let last = match stop {
                    XrangeIdInput::MsTime(ms_time) => {
                        let id = Id {
                            ms_time: *ms_time as usize,
                            sequence: 0,
                        };
                        stream.partition_point(|key_id, _value| key_id <= &id)
                    }
                    XrangeIdInput::Id(id) => {
                        if let Some(idx) = stream.get_index_of(id) {
                            idx
                        } else {
                            todo!()
                        }
                    }
                };

                if let Some(range) = stream.get_range(first..=last) {
                    range
                        .iter()
                        .map(|(key, value)| DatabaseStreamEntry {
                            id: *key,
                            values: value.clone(),
                        })
                        .collect()
                } else {
                    vec![]
                }
            } else {
                todo!()
            }
        } else {
            vec![]
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DbStreamAddError {
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    IdNotGreater,
    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    IdZeroZero,
    #[error("ERR Couldn't generate UNIX time: {0}")]
    TimeError(#[from] std::time::SystemTimeError),
}
