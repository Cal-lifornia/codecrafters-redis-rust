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
    pub async fn prepend_list(&self, key: &Bytes, values: Vec<Bytes>) -> i64 {
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            for value in values {
                list.push_front(value);
            }
            list.len() as i64
        } else {
            let list = VecDeque::from_iter(values.iter().cloned().rev());
            let len = list.len() as i64;
            lists.insert(key.clone(), list);
            len
        }
    }
    pub async fn range_list(&self, key: &Bytes, start: i64, stop: i64) -> Vec<Bytes> {
        let lists = self.lists.read().await;
        if let Some(list) = lists.get(key) {
            let mut start = start;
            let mut stop = stop;
            if start.is_negative() {
                start = (list.len() as i64 + start).max(0);
            }
            if stop.is_negative() {
                stop = (list.len() as i64 + stop).max(0);
            }
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
    pub async fn list_len(&self, key: &Bytes) -> i64 {
        let lists = self.lists.read().await;
        if let Some(list) = lists.get(key) {
            list.len() as i64
        } else {
            0
        }
    }

    pub async fn pop_list(&self, key: &Bytes, count: Option<u64>) -> Vec<Bytes> {
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            if let Some(count) = count {
                todo!()
            } else {
                if let Some(out) = list.pop_front() {
                    vec![out]
                } else {
                    vec![]
                }
            }
        } else {
            vec![]
        }
    }
}
