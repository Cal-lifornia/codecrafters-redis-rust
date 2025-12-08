use bytes::Bytes;
use redis_proc_macros::RedisCommand;

#[derive(RedisCommand)]
#[redis_command(syntax = "GET key")]
pub struct Get {
    key: Bytes,
}
