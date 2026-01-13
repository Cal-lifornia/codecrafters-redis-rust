use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    Pair,
    command::AsyncCommand,
    database::Location,
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
        let location = Location::new(self.latitude, self.longitude)?.encode();
        let num = ctx
            .db
            .insert_set_member(self.key.clone(), self.member.clone(), location as f64)
            .await;
        RespType::Integer(num as i64).write_to_buf(buf);
        Ok(())
    }
}
#[derive(RedisCommand)]
#[redis_command(syntax = "GEOPOS key member")]
pub struct Geopos {
    key: Bytes,
    members: Vec<Bytes>,
}

#[async_trait]
impl AsyncCommand for Geopos {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        let mut results = vec![];
        for member in &self.members {
            if let Some(geo_code) = ctx.db.get_set_member_score(&self.key, member).await {
                results.push(RespType::from(Location::decode(geo_code as u64)));
            } else {
                results.push(RespType::NullArray);
            }
        }
        results.write_to_buf(buf);
        Ok(())
    }
}
