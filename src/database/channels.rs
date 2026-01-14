use std::net::SocketAddr;

use bytes::Bytes;
use hashbrown::{HashMap, HashSet};

use crate::context::ConnWriter;
use crate::redis::RedisError;

// #[derive(Debug, Clone)]
// struct ChannelSub {
//     addr: SocketAddr,
//     writer: ConnWriter,
// }

// impl Hash for ChannelSub {
//     fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//         self.addr.hash(state);
//     }
// }

// impl Eq for ChannelSub {}

// impl PartialEq for ChannelSub {
//     fn eq(&self, other: &Self) -> bool {
//         self.addr == other.addr
//     }
// }

#[derive(Default)]
pub struct ChannelDB {
    channels: HashMap<Bytes, HashMap<SocketAddr, ConnWriter>>,
    subscriptions: HashMap<SocketAddr, HashSet<Bytes>>,
}

// #[derive(Default)]
// pub struct ChannelDB {
//     channels: HashTable<Pair<ConnWriter, SocketAddr>>,
//     subscriptions: HashMap<SocketAddr, HashSet<Bytes>>,
//     hasher: DefaultHashBuilder,
// }

impl ChannelDB {
    pub async fn subscribe_to_channel(
        &mut self,
        channel: Bytes,
        writer: ConnWriter,
    ) -> Result<usize, RedisError> {
        let addr = writer.read().await.peer_addr()?;
        tracing::debug!("ADDR: {addr}");
        let chan = self.channels.entry(channel.clone()).or_default();
        if chan.contains_key(&addr) {
            Ok(self.num_channels(&writer).await?)
        } else {
            chan.insert(addr, writer.clone());
            self.subscriptions
                .entry(addr)
                .or_default()
                .insert(channel.clone());
            Ok(self.num_channels(&writer).await?)
        }
    }

    pub async fn num_channels(&self, writer: &ConnWriter) -> std::io::Result<usize> {
        let addr = writer.read().await.peer_addr()?;
        if let Some(subs) = self.subscriptions.get(&addr) {
            Ok(subs.len())
        } else {
            Ok(0)
        }
    }

    pub async fn subscribed(&self, writer: &ConnWriter) -> std::io::Result<bool> {
        let addr = writer.read().await.peer_addr()?;
        if let Some(list) = self.subscriptions.get(&addr) {
            Ok(!list.is_empty())
        } else {
            Ok(false)
        }
    }

    pub fn get_channel_writers(&self, channel: &Bytes) -> Vec<ConnWriter> {
        if let Some(channel) = self.channels.get(channel) {
            channel.values().cloned().collect()
        } else {
            vec![]
        }
    }

    pub async fn unsubscribe_from_channel(
        &mut self,
        channel: &Bytes,
        writer: &ConnWriter,
    ) -> std::io::Result<usize> {
        tracing::debug!("Unsubscribing");
        let addr = writer.read().await.peer_addr()?;
        if let Some(chan) = self.channels.get_mut(channel)
            && chan.remove(&addr).is_some()
            && let Some(list) = self.subscriptions.get_mut(&addr)
        {
            list.remove(channel);
        }
        self.num_channels(writer).await
    }

    // pub async fn subscribe_to_channel(
    //     &mut self,
    //     channel: Bytes,
    //     writer: ConnWriter,
    // ) -> Result<usize, RedisError> {
    //     let addr = writer.read().await.peer_addr()?;
    //     tracing::debug!("ADDR: {addr}");
    //     let hash = self.hasher.hash_one(&channel);
    //     if self.channels.find(hash, |val| val.right == addr).is_some() {
    //         Ok(self.num_channels(&writer).await?)
    //     } else {
    //         self.channels
    //             .insert_unique(hash, Pair::new(writer, addr), |_val| hash);
    //         let chan_subs = self.subscriptions.entry(addr).or_default();
    //         chan_subs.insert(channel.clone());
    //         Ok(chan_subs.len())
    //     }
    // }

    // pub async fn num_channels(&self, writer: &ConnWriter) -> std::io::Result<usize> {
    //     let addr = writer.read().await.peer_addr()?;
    //     if let Some(subs) = self.subscriptions.get(&addr) {
    //         Ok(subs.len())
    //     } else {
    //         Ok(0)
    //     }
    // }

    // pub fn get_channel_writers(&self, channel: &Bytes) -> Vec<ConnWriter> {
    //     let hash = self.hasher.hash_one(channel);
    //     let out = self
    //         .channels
    //         .iter_hash(hash)
    //         .map(|val| val.left.clone())
    //         .collect::<Vec<ConnWriter>>();
    //     tracing::debug!("WRITERS: {out:#?}");
    //     out
    // }

    // pub async fn unsubscribe_from_channel(
    //     &mut self,
    //     channel: &Bytes,
    //     writer: &ConnWriter,
    // ) -> std::io::Result<usize> {
    //     tracing::debug!("Unsubscribing");
    //     let addr = writer.read().await.peer_addr()?;
    //     let hash = self.hasher.hash_one(channel);
    //     if let Ok(entry) = self.channels.find_entry(hash, |val| val.right == addr) {
    //         entry.remove();
    //         if let Some(list) = self.subscriptions.get_mut(&addr) {
    //             list.remove(channel);
    //         }
    //     }
    //     self.num_channels(writer).await
    // }
}
