use bytes::Bytes;

use crate::database::RedisDatabase;

#[derive(Clone, Copy)]
pub struct Location {
    longitude: f64,
    latitude: f64,
}
impl Location {
    pub fn new(long: f64, lat: f64) -> Result<Self, LocationError> {
        if !(-180.0..=180.0).contains(&long) || !(-85.05112878..=85.05112878).contains(&lat) {
            return Err(LocationError::InvalidLongLatPair(long, lat));
        }

        Ok(Self {
            longitude: long,
            latitude: lat,
        })
    }
    pub fn longitude(&self) -> f64 {
        self.longitude
    }
    pub fn latitude(&self) -> f64 {
        self.latitude
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LocationError {
    #[error("invalid longitude,latitude pair {0},{1}")]
    InvalidLongLatPair(f64, f64),
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
