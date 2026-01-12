#[derive(Clone, Copy)]
pub struct Location {
    latitude: f64,
    longitude: f64,
}
impl Location {
    pub const MIN_LATITUDE: f64 = -85.05112878;
    pub const MAX_LATITUDE: f64 = 85.05112878;
    pub const MIN_LONGITUDE: f64 = -180.0;
    pub const MAX_LONGITUDE: f64 = 180.0;

    pub const LATITUDE_RANGE: f64 = Self::MAX_LATITUDE - Self::MIN_LATITUDE;
    pub const LONGITUDE_RANGE: f64 = Self::MAX_LONGITUDE - Self::MIN_LONGITUDE;

    pub fn new(latitude: f64, longitude: f64) -> Result<Self, LocationError> {
        tracing::debug!("LAT: {latitude}; LONG: {longitude}");
        if !(Self::MIN_LATITUDE..=Self::MAX_LATITUDE).contains(&latitude)
            || !(Self::MIN_LONGITUDE..=Self::MAX_LONGITUDE).contains(&longitude)
        {
            return Err(LocationError::InvalidLongLatPair(longitude, latitude));
        }

        Ok(Self {
            latitude,
            longitude,
        })
    }
    pub fn longitude(&self) -> f64 {
        self.longitude
    }
    pub fn latitude(&self) -> f64 {
        self.latitude
    }
    pub fn encode(&self) -> u64 {
        // Normalize to the range 0-2^26
        let normalized_latitude =
            2.0_f64.powi(26) * (self.latitude - Self::MIN_LATITUDE) / Self::LATITUDE_RANGE;
        let normalized_longitude =
            2f64.powi(26) * (self.longitude - Self::MIN_LONGITUDE) / Self::LONGITUDE_RANGE;

        // Truncate to integers
        let lat_int = normalized_latitude as u32;
        let lon_int = normalized_longitude as u32;

        interleave(lat_int, lon_int)
    }
    fn convert_grid_numbers_to_coordinates(
        grid_latitude_number: u32,
        grid_longitude_number: u32,
    ) -> Self {
        // Calculate the grid boundaries
        let grid_latitude_min = Self::MIN_LATITUDE
            + Self::LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
        let grid_latitude_max = Self::MIN_LATITUDE
            + Self::LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
        let grid_longitude_min = Self::MIN_LONGITUDE
            + Self::LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
        let grid_longitude_max = Self::MIN_LONGITUDE
            + Self::LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));

        // Calculate the center point of the grid cell
        let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
        let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

        Self {
            latitude,
            longitude,
        }
    }
    pub fn decode(geo_code: u64) -> Self {
        // Align bits of both latitude and longitude to take even-numbered position
        let y = geo_code >> 1;
        let x = geo_code;

        // Compact bits back to 32-bit ints
        let grid_latitude_number = compact_int64_to_int32(x);
        let grid_longitude_number = compact_int64_to_int32(y);

        Self::convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
    }
}

impl TryFrom<(f64, f64)> for Location {
    type Error = LocationError;
    fn try_from(value: (f64, f64)) -> Result<Self, Self::Error> {
        Self::new(value.0, value.1)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LocationError {
    #[error("invalid longitude,latitude pair {0},{1}")]
    InvalidLongLatPair(f64, f64),
}

fn spread_int32_to_int64(v: u32) -> u64 {
    let mut result = v as u64;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    (result | (result << 1)) & 0x5555555555555555
}

fn interleave(x: u32, y: u32) -> u64 {
    let x_spread = spread_int32_to_int64(x);
    let y_spread = spread_int32_to_int64(y);
    let y_shifted = y_spread << 1;
    x_spread | y_shifted
}

fn compact_int64_to_int32(v: u64) -> u32 {
    let mut result = v & 0x5555555555555555;
    result = (result | (result >> 1)) & 0x3333333333333333;
    result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
    result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
    ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32 // Cast to u32
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("Bangkok", 13.7220, 100.5252, 3962257306574459)]
    #[case("Beijing", 39.9075, 116.3972, 4069885364908765)]
    #[case("Berlin", 52.5244, 13.4105, 3673983964876493)]
    #[case("Copenhagen", 55.6759, 12.5655, 3685973395504349)]
    #[case("New Delhi", 28.6667, 77.2167, 3631527070936756)]
    #[case("Kathmandu", 27.7017, 85.3206, 3639507404773204)]
    #[case("London",51.5074,-0.1278,2163557714755072)]
    #[case("New York",40.7128,-74.0060,1791873974549446)]
    #[case("Paris", 48.8534, 2.3488, 3663832752681684)]
    #[case("Sydney",-33.8688,151.2093,3252046221964352)]
    fn test_encode(
        #[case] name: &'static str,
        #[case] latitude: f64,
        #[case] longitude: f64,
        #[case] expected_score: u64,
    ) {
        let actual_score = Location::new(latitude, longitude).unwrap().encode();
        assert_eq!(actual_score, expected_score, "Failed {name}")
    }
}
