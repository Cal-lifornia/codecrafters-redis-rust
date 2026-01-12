use bytes::Bytes;

use crate::database::RedisDatabase;

#[derive(Clone, Copy)]
pub struct Location {
    pub longitude: f64,
    pub latitude: f64,
}
impl Location {
    pub fn new(long: f64, lat: f64) -> Self {
        Self {
            longitude: long,
            latitude: lat,
        }
    }
}

impl RedisDatabase {
    pub async fn insert_geo_location(
        &self,
        key: Bytes,
        member: Bytes,
        location: Location,
    ) -> usize {
        let mut geo = self.geo.write().await;
        let set = geo.entry(key).or_default();
        if set.insert(member, location).is_some() {
            0
        } else {
            1
        }
    }
}
