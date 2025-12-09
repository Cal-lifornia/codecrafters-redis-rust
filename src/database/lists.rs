use std::{collections::VecDeque, ops::RangeBounds};

use bytes::Bytes;

use crate::database::RedisDatabase;

impl RedisDatabase {
    pub async fn push_list(&self, key: &Bytes, values: Vec<Bytes>) -> i64 {
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            list.extend(values.iter().cloned());
            list.len() as i64
        } else {
            let list = VecDeque::from(values);
            let len = list.len();
            lists.insert(key.clone(), list);
            len as i64
        }
    }
    pub async fn range_list(&self, key: &Bytes, start: i64, stop: i64) -> Vec<Bytes> {
        let lists = self.lists.read().await;
        if let Some(list) = lists.get(key) {
            if start > stop || start as usize >= list.len() {
                vec![]
            } else {
                list.range(start as usize..=(stop as usize).min(list.len() - 1))
                    .cloned()
                    .collect()
            }
        } else {
            vec![]
        }
    }
}
