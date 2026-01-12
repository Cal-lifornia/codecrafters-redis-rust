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

impl TryFrom<(f64, f64)> for Location {
    type Error = LocationError;
    fn try_from(value: (f64, f64)) -> Result<Self, Self::Error> {
        Self::new(value.0, value.1)
    }
}

impl From<Location> for f64 {
    fn from(value: Location) -> Self {
        value.longitude.to_radians() + value.latitude.to_radians()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LocationError {
    #[error("invalid longitude,latitude pair {0},{1}")]
    InvalidLongLatPair(f64, f64),
}
