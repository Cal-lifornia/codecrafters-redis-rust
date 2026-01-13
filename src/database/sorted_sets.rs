use bytes::Bytes;

use crate::database::{Coordinates, RedisDatabase};

impl RedisDatabase {
    pub async fn insert_set_member(&self, key: Bytes, member: Bytes, score: f64) -> usize {
        let mut sets = self.sets.write().await;
        let set = sets.entry(key).or_default();
        if let Some(current_score) = set.get_mut(&member) {
            *current_score = score;
            // set.sort_by(|curr_mem, curr_score, other_mem, other_score| {
            //     curr_score
            //         .total_cmp(other_score)
            //         .then(curr_mem.cmp(other_mem))
            // });
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
    pub async fn count_sorted_set(&self, key: &Bytes) -> usize {
        let sets = self.sets.read().await;
        if let Some(set) = sets.get(key) {
            set.len()
        } else {
            0
        }
    }

    pub async fn get_set_member_score(&self, key: &Bytes, member: &Bytes) -> Option<f64> {
        let sets = self.sets.read().await;
        if let Some(set) = sets.get(key) {
            set.get(member).copied()
        } else {
            None
        }
    }
    pub async fn remove_set_member(&self, key: &Bytes, member: &Bytes) -> usize {
        let mut sets = self.sets.write().await;
        if let Some(set) = sets.get_mut(key) {
            if set.shift_remove(member).is_some() {
                1
            } else {
                0
            }
        } else {
            0
        }
    }
    pub async fn get_distance(&self, key: &Bytes, first: &Bytes, second: &Bytes) -> Option<f64> {
        let sets = self.sets.read().await;
        if let Some(set) = sets.get(key) {
            if let Some(first_geo) = set.get(first)
                && let Some(second_geo) = set.get(second)
            {
                let first_coord = Coordinates::decode(*first_geo as u64);
                let second_coord = Coordinates::decode(*second_geo as u64);
                Some(first_coord.distance(&second_coord))
            } else {
                None
            }
        } else {
            None
        }
    }
}
