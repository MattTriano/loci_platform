//! Left-turn penalty model.
//!
//! Mirrors `compute_left_turn_penalty` and the constants in routing.py.
//! The penalty is applied at routing time, scaled by the road class of
//! the edge being turned onto.

use crate::format::{Edge, Graph};
use crate::geom::{bearing, is_left_turn, turn_angle_deg, MIN_TURN_ANGLE_DEG};

/// Base penalty in "virtual stress-cost meters" before road-class scaling.
/// Matches `_LEFT_TURN_BASE_PENALTY` in routing.py.
pub const LEFT_TURN_BASE_PENALTY: f32 = 30.0;

/// Default multiplier for highway types not in the table.
pub const DEFAULT_ROAD_MULTIPLIER: f32 = 1.0;

/// Multiplier for a given OSM `highway` value. Matches the
/// `_ROAD_CLASS_MULTIPLIER` dict in routing.py. Returns
/// `DEFAULT_ROAD_MULTIPLIER` for unknown values.
pub fn road_class_multiplier(highway: Option<&str>) -> f32 {
    match highway {
        Some("primary") => 2.5,
        Some("primary_link") => 2.0,
        Some("secondary") => 2.0,
        Some("secondary_link") => 1.5,
        Some("tertiary") => 1.5,
        Some("tertiary_link") => 1.2,
        Some("residential") => 0.8,
        Some("living_street") => 0.5,
        Some("service") => 0.5,
        Some("cycleway") => 0.0,
        Some("path") => 0.0,
        Some("pedestrian") => 0.0,
        _ => DEFAULT_ROAD_MULTIPLIER,
    }
}

/// Compute the left-turn penalty for the transition `prev → curr → next`.
///
/// Returns 0 if:
///   - `curr` is not an intersection (degree < 3)
///   - the turn angle is too small (gentle curve)
///   - the turn is a right turn
///   - the target road has no oncoming motor traffic (multiplier = 0)
///
/// `next_edge` is the edge being entered; its `highway` value determines
/// the multiplier.
pub fn compute_left_turn_penalty(
    graph: &Graph,
    prev_idx: u32,
    curr_idx: u32,
    next_idx: u32,
    next_edge: &Edge,
    is_intersection: bool,
) -> f32 {
    if !is_intersection {
        return 0.0;
    }

    let prev = graph.nodes[prev_idx as usize];
    let curr = graph.nodes[curr_idx as usize];
    let next = graph.nodes[next_idx as usize];

    let bearing_in = bearing(prev.lon, prev.lat, curr.lon, curr.lat);
    let bearing_out = bearing(curr.lon, curr.lat, next.lon, next.lat);

    let angle = turn_angle_deg(bearing_in, bearing_out);
    if angle < MIN_TURN_ANGLE_DEG {
        return 0.0;
    }

    if !is_left_turn(bearing_in, bearing_out) {
        return 0.0;
    }

    let highway = graph.lookup_str(next_edge.highway_str_idx);
    let multiplier = road_class_multiplier(highway);
    if multiplier == 0.0 {
        return 0.0;
    }

    LEFT_TURN_BASE_PENALTY * multiplier
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_road_has_highest_multiplier() {
        assert!(
            road_class_multiplier(Some("primary")) > road_class_multiplier(Some("residential"))
        );
    }

    #[test]
    fn cycleway_is_zero() {
        assert_eq!(road_class_multiplier(Some("cycleway")), 0.0);
    }

    #[test]
    fn unknown_highway_uses_default() {
        assert_eq!(
            road_class_multiplier(Some("motorway")),
            DEFAULT_ROAD_MULTIPLIER
        );
        assert_eq!(road_class_multiplier(None), DEFAULT_ROAD_MULTIPLIER);
    }
}
