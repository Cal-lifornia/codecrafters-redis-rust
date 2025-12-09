use std::collections::VecDeque;

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
}
