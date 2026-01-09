use bytes::Bytes;

use crate::{context::ConnWriter, database::RedisDatabase};

impl RedisDatabase {
    pub async fn subscibe_to_channel(&self, channel: Bytes, writer: ConnWriter) {
        let mut channels = self.channels.write().await;
        let channel = channels.entry(channel).or_default();
        channel.push(writer);
    }
}
