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
            set.insert_sorted_by(
                member,
                score,
                |curr_mem, curr_score, other_mem, other_score| {
                    curr_score
                        .total_cmp(other_score)
                        .then(curr_mem.cmp(other_mem))
                },
            );
            1
        }
    }
    pub async fn get_rank(&self, key: &Bytes, member: &Bytes) -> Option<usize> {
        let sets = self.sets.read().await;
        if let Some(set) = sets.get(key) {
            set.get_index_of(member)
        } else {
            None
        }
    }
}
