use async_trait::async_trait;
use bytes::Bytes;
use hashbrown::HashMap;
use redis_proc_macros::RedisCommand;

use crate::{command::AsyncCommand, id::WildcardID, resp::RedisWrite};

#[derive(RedisCommand)]
#[redis_command(
    syntax = "XADD key [NOMKSTREAM] [KEEPREF | DELREF | ACKED] [<MAXLEN | MINID> [= | ~] threshold
  [LIMIT count]] <* | id> field value [field value ...]"
)]
pub struct Xadd {
    key: Bytes,
    id: WildcardID,
    values: HashMap<Bytes, Bytes>,
}

#[async_trait]
impl AsyncCommand for Xadd {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let id = ctx
            .db
            .add_stream(&self.key, self.id.clone(), self.values.clone())
            .await;
        id.write_to_buf(buf);
        Ok(())
    }
}
