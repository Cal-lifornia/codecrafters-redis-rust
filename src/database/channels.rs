use std::hash::BuildHasher;
use std::net::SocketAddr;

use bytes::Bytes;
use hashbrown::{DefaultHashBuilder, HashMap, HashTable};

use crate::Pair;
use crate::context::ConnWriter;
use crate::server::RedisError;

#[derive(Default)]
pub struct ChannelDB {
    channels: HashTable<Pair<ConnWriter, SocketAddr>>,
    subscriptions: HashMap<SocketAddr, Vec<usize>>,
}

impl ChannelDB {
    pub async fn subscribe_to_channel(
        &mut self,
        channel: Bytes,
        writer: ConnWriter,
    ) -> Result<usize, RedisError> {
        let addr = writer.read().await.peer_addr()?;
        let hasher = DefaultHashBuilder::default();
        let hash = hasher.hash_one(channel);
        if self.channels.find(hash, |val| val.right == addr).is_some() {
            Ok(self.num_channels(writer).await?)
        } else {
            self.channels
                .insert_unique(hash, Pair::new(writer, addr), |_val| hash);
            if let Some(index) = self
                .channels
                .find_bucket_index(hash, |val| val.right == addr)
            {
                let chan_subs = self.subscriptions.entry(addr).or_default();
                chan_subs.push(index);
                Ok(chan_subs.len())
            } else {
                Err(RedisError::Other("Failed to find bucket index".into()))
            }
        }
    }

    pub async fn num_channels(&self, writer: ConnWriter) -> std::io::Result<usize> {
        let addr = writer.read().await.peer_addr()?;
        if let Some(subs) = self.subscriptions.get(&addr) {
            Ok(subs.len())
        } else {
            Ok(0)
        }
    }
}
