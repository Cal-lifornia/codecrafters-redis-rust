use bytes::Bytes;

use crate::database::RedisDatabase;

impl RedisDatabase {
    pub async fn insert_sorted_set(&self, key: Bytes, member: Bytes, score: f64) -> usize {
        let mut sets = self.sets.write().await;
        let set = sets.entry(key).or_default();
        if let Some(current_score) = set.get_mut(&member) {
            *current_score = score;
            0
        } else {
            set.insert(member, score);
            1
        }
    }
}
