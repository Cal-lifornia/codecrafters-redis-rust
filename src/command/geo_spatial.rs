use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::{AsyncCommand, SymbolByRadius, SymbolFromLonLat},
    database::Coordinates,
    resp::{NullBulkString, RedisWrite, RespType},
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
    ) -> Result<(), crate::redis::RedisError> {
        let location = Coordinates::new(self.latitude, self.longitude)?.encode();
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
    ) -> Result<(), crate::redis::RedisError> {
        let mut results = vec![];
        for member in &self.members {
            if let Some(geo_code) = ctx.db.get_set_member_score(&self.key, member).await {
                results.push(RespType::from(Coordinates::decode(geo_code as u64)));
            } else {
                results.push(RespType::NullArray);
            }
        }
        results.write_to_buf(buf);
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "GEODIST key first second")]
pub struct Geodist {
    key: Bytes,
    first: Bytes,
    second: Bytes,
}

#[async_trait]
impl AsyncCommand for Geodist {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError> {
        if let Some(distance) = ctx
            .db
            .get_distance(&self.key, &self.first, &self.second)
            .await
        {
            RespType::bulk_string(distance.to_string()).write_to_buf(buf);
        } else {
            NullBulkString.write_to_buf(buf);
        }
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "GEOSEARCH key FROMONLAT longitude latitude BYRADIUS radius m")]
pub struct Geosearch {
    key: Bytes,
    #[allow(unused)]
    fromlonlat: SymbolFromLonLat,
    longitude: f64,
    latitude: f64,
    #[allow(unused)]
    byradius: SymbolByRadius,
    radius: f64,
    #[allow(unused)]
    metre: Bytes,
}

#[async_trait]
impl AsyncCommand for Geosearch {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError> {
        let results = ctx
            .db
            .search_area(
                &self.key,
                Coordinates::new(self.latitude, self.longitude)?,
                self.radius,
            )
            .await;
        results.write_to_buf(buf);
        Ok(())
    }
}
