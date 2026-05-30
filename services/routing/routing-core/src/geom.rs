//! /loci_platform/services/routing/routing-core/src/geom.rs
//! Geometry helpers used by the routing layer.
//!
//! Pure functions, no graph dependencies. Bearings use the
//! `atan2(dx, dy)` convention from the Python original — i.e. zero
//! is "north" (dy positive), positive is "east of north" (dx positive).
//! This is a math-bearing, not a compass-bearing — but as long as the
//! Python and Rust sides use the same convention, the turn-angle and
//! left-vs-right computations agree.
//!
//! All coordinate-derived math is f64, matching the f64 node
//! coordinates in the graph format. Turn penalties downstream are f32,
//! but the angle math feeding them is f64.

/// Minimum unsigned turn angle (degrees) to count as a real turn
/// rather than a gentle curve. Matches `_MIN_TURN_ANGLE_DEG` in
/// routing.py.
pub const MIN_TURN_ANGLE_DEG: f64 = 45.0;

/// Bearing in radians from `(from_lon, from_lat)` to `(to_lon, to_lat)`.
///
/// Matches the Python definition:
///   atan2(t.x - f.x, t.y - f.y)
/// where x is longitude and y is latitude.
pub fn bearing(from_lon: f64, from_lat: f64, to_lon: f64, to_lat: f64) -> f64 {
    (to_lon - from_lon).atan2(to_lat - from_lat)
}

/// Unsigned turn angle in degrees between two bearings.
/// 0 = straight ahead, 180 = U-turn.
pub fn turn_angle_deg(bearing_in: f64, bearing_out: f64) -> f64 {
    let diff = (bearing_out - bearing_in).to_degrees().abs() % 360.0;
    if diff <= 180.0 {
        diff
    } else {
        360.0 - diff
    }
}

/// True if the transition from `bearing_in` to `bearing_out` is a left turn.
///
/// Matches the Python implementation: in a (lon = x, lat = y) frame in
/// the northern hemisphere, a positive cross product between the
/// incoming and outgoing direction vectors means turning left.
pub fn is_left_turn(bearing_in: f64, bearing_out: f64) -> bool {
    let dx_in = bearing_in.sin();
    let dy_in = bearing_in.cos();
    let dx_out = bearing_out.sin();
    let dy_out = bearing_out.cos();
    let cross = dx_in * dy_out - dy_in * dx_out;
    cross > 0.0
}

/// Great-circle distance in meters between two (lat, lon) points.
pub fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6_371_000.0_f64;
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let dphi = (lat2 - lat1).to_radians();
    let dlam = (lon2 - lon1).to_radians();
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlam / 2.0).sin().powi(2);
    r * 2.0 * a.sqrt().atan2((1.0 - a).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn straight_line_is_zero_turn() {
        let b1 = bearing(0.0, 0.0, 0.0, 1.0); // due north
        let b2 = bearing(0.0, 1.0, 0.0, 2.0); // still due north
        assert!(turn_angle_deg(b1, b2) < 0.001);
    }

    #[test]
    fn ninety_degree_turn() {
        let b_in = bearing(0.0, 0.0, 0.0, 1.0); // north
        let b_out = bearing(0.0, 1.0, 1.0, 1.0); // east
        let angle = turn_angle_deg(b_in, b_out);
        assert!((angle - 90.0).abs() < 0.01, "got {angle}");
    }

    #[test]
    fn left_turn_detected_north_then_west() {
        let b_in = bearing(0.0, 0.0, 0.0, 1.0); // north
        let b_out = bearing(0.0, 1.0, -1.0, 1.0); // west
        assert!(is_left_turn(b_in, b_out));
    }

    #[test]
    fn right_turn_not_detected_as_left() {
        let b_in = bearing(0.0, 0.0, 0.0, 1.0); // north
        let b_out = bearing(0.0, 1.0, 1.0, 1.0); // east
        assert!(!is_left_turn(b_in, b_out));
    }

    #[test]
    fn haversine_known_distance() {
        // Chicago to NYC: ~1145 km
        let d = haversine_m(41.8781, -87.6298, 40.7128, -74.0060);
        assert!((d - 1_145_000.0).abs() < 10_000.0, "got {d}m");
    }
}
