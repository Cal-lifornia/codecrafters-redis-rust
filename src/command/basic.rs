use bytes::{Bytes, BytesMut};
use redis_proc_macros::RedisCommand;

use crate::resp::{RedisWrite, RespType};

#[derive(RedisCommand)]
#[redis_command(syntax = "ECHO message")]
pub struct Echo {
    message: Bytes,
}

impl Echo {
    pub async fn run(&self, buf: &mut BytesMut) -> std::io::Result<()> {
        RespType::BulkString(self.message.clone()).write_to_buf(buf);
        Ok(())
    }
}
