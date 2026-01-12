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
    pub async fn get_set_member_rank(&self, key: &Bytes, member: &Bytes) -> Option<usize> {
        let sets = self.sets.read().await;
        if let Some(set) = sets.get(key) {
            set.get_index_of(member)
        } else {
            None
        }
    }
    pub async fn range_sorted_set(&self, key: &Bytes, start: i64, end: i64) -> Vec<Bytes> {
        let sets = self.sets.read().await;
        if let Some(set) = sets.get(key) {
            let end = if end.is_negative() {
                (set.len()).saturating_sub(end.unsigned_abs() as usize)
            } else {
                (set.len() - 1).min(end as usize)
            };
            let start = if start.is_negative() {
                (set.len()).saturating_sub((start.unsigned_abs()) as usize)
            } else {
                start as usize
            };
            if start > end {
                return vec![];
            }
            if let Some(value) = set.get_range(start..=end) {
                value.keys().cloned().collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }
}
