use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::AsyncCommand,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "GEOADD key longitude latitude member")]
pub struct Geoadd {
    key: Bytes,
    longitude: f64,
    latitude: f64,
    member: Bytes,
}

#[async_trait]
impl AsyncCommand for Geoadd {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        let location = crate::database::Location {
            longitude: self.longitude,
            latitude: self.latitude,
        };
        let num = ctx
            .db
            .insert_geo_location(self.key.clone(), self.member.clone(), location)
            .await;
        RespType::Integer(num as i64).write_to_buf(buf);
        Ok(())
    }
}
