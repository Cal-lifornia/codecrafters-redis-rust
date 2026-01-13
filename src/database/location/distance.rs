use crate::database::Coordinates;

/// Earth's radius in metres
const EARTH_RADIUS: f64 = 6372797.560856;

// https://en.wikipedia.org/wiki/Haversine_formula
// The basic Haversine formula is d=r𝜃 where
// 'd' is the distance in metres between two coordinates on a sphere,
// 'r' is the radius of the sphere
// and
// '𝜃' is central angel between two points on a sphere

impl Coordinates {
    pub fn distance(&self, other: &Self) -> f64 {
        // Currently the longitudes are in degrees, need to get them to radians

        let long1_rad = self.longitude().to_radians();
        let long2_rad = other.longitude().to_radians();
        let v = ((long2_rad - long1_rad) / 2.0).sin();
        if v == 0.0 {
            get_lat_distance(self.latitude(), other.latitude())
        } else {
            let lat1_rad = self.latitude().to_radians();
            let lat2_rad = other.latitude().to_radians();
            let u = ((lat2_rad - lat1_rad) / 2.0).sin();
            let a = u * u + lat1_rad.cos() * lat2_rad.cos() * v * v;
            2.0 * EARTH_RADIUS * a.sqrt().asin()
        }
    }
}

fn get_lat_distance(lat_deg: f64, other_lat_deg: f64) -> f64 {
    EARTH_RADIUS * (other_lat_deg.to_radians() - lat_deg.to_radians()).abs()
}
