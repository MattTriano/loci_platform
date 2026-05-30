//! /loci_platform/services/routing/routing-core/src/turn_cost.rs
//! Left-turn penalty model.
//!
//! Mirrors `compute_left_turn_penalty` and the constants in routing.py.
//! The penalty is applied at routing time, scaled by the most stressful
//! road class *involved in the maneuver* — the larger of the road being
//! left and the road being entered (see `compute_left_turn_penalty`).

use crate::format::{Edge, Graph};
use crate::geom::{bearing, is_left_turn, turn_angle_deg, MIN_TURN_ANGLE_DEG};

/// Base penalty in "virtual stress-cost meters" before road-class scaling.
/// Matches `_LEFT_TURN_BASE_PENALTY` in routing.py.
pub const LEFT_TURN_BASE_PENALTY: f32 = 50.0;

/// Default multiplier for highway types not in the table.
pub const DEFAULT_ROAD_MULTIPLIER: f32 = 1.0;

/// Multiplier for a given OSM `highway` value. Matches the
/// `_ROAD_CLASS_MULTIPLIER` dict in routing.py. Returns
/// `DEFAULT_ROAD_MULTIPLIER` for unknown values.
pub fn road_class_multiplier(highway: Option<&str>) -> f32 {
    match highway {
        Some("primary") => 3.5,
        Some("primary_link") => 3.0,
        Some("secondary") => 2.5,
        Some("secondary_link") => 2.2,
        Some("tertiary") => 1.8,
        Some("tertiary_link") => 1.6,
        Some("residential") => 0.6,
        Some("living_street") => 0.4,
        Some("service") => 0.3,
        Some("cycleway") => 0.0,
        Some("path") => 0.0,
        Some("pedestrian") => 0.0,
        _ => DEFAULT_ROAD_MULTIPLIER,
    }
}

/// Compute the left-turn penalty for the transition `prev → curr → next`.
///
/// Returns 0 if:
///   - `curr` is not an intersection
///   - the turn angle is too small (gentle curve)
///   - the turn is a right turn
///   - both roads involved have multiplier 0 (e.g. cycleway → path)
///
/// The stress of a left turn is set by the most stressful road *involved
/// in the maneuver*, not just the road being entered. There are two ways
/// a primary road makes a left dangerous, and we want both to count:
///   - Turning left FROM a primary: you cross its oncoming traffic.
///   - Turning left ONTO a primary: you merge into its traffic.
/// So we take the larger of the incoming and outgoing road-class
/// multipliers. (Previously this used only the outgoing edge, which
/// under-counted lefts off a high-class road onto a small one — e.g. a
/// left from a primary onto a path scored as a path, ~0.)
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

    let out_mult = road_class_multiplier(graph.lookup_str(next_edge.highway_str_idx));
    let in_mult = incoming_multiplier(graph, prev_idx, curr_idx);
    let multiplier = out_mult.max(in_mult);

    LEFT_TURN_BASE_PENALTY * multiplier
}

/// Road-class multiplier of the edge that was traversed to reach `curr`
/// from `prev`, i.e. the road being turned *off of*.
///
/// Out-degree at a street node is tiny, so this linear scan is cheap. If
/// parallel edges connect `prev → curr`, take the most stressful class
/// among them. Returns 0.0 if no such edge is found (which shouldn't
/// happen on a real path); since the caller takes `max(out, in)`, a 0.0
/// here simply falls back to the outgoing-edge multiplier — the old
/// behavior — rather than inflating or zeroing the penalty.
fn incoming_multiplier(graph: &Graph, prev_idx: u32, curr_idx: u32) -> f32 {
    graph
        .edges_from(prev_idx)
        .iter()
        .filter(|e| e.target_node_idx == curr_idx)
        .map(|e| road_class_multiplier(graph.lookup_str(e.highway_str_idx)))
        .fold(0.0_f32, f32::max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Edge, Graph, Node};

    fn node(osm_id: u64, lon: f64, lat: f64, is_intersection: bool) -> Node {
        Node {
            osm_id,
            lon,
            lat,
            is_intersection,
        }
    }

    fn edge(target: u32, highway_str_idx: u32) -> Edge {
        Edge {
            target_node_idx: target,
            segment_id_str_idx: 0,
            name_str_idx: u32::MAX,
            highway_str_idx,
            infra_type_str_idx: u32::MAX,
            flags: 0b1,
            length_m: 100.0,
            stress_cost: 100.0,
            physical_cost: 100.0,
            intersection_cost: 0.0,
            crash_cost: 0.0,
        }
    }

    /// Build a 3-node graph forming a turn at the middle node:
    ///   node 0 (prev) at (lon=0,  lat=0)
    ///   node 1 (curr) at (lon=0,  lat=1)   <- the potential intersection
    ///   node 2 (next) at `next`
    /// Heading north 0→1, then on to `next`. With next=(-1,1) this is a
    /// 90° left; (1,1) a right; (0,2) straight ahead.
    /// `in_hwy` is the class of edge 0→1, `out_hwy` the class of 1→2.
    fn turn_graph(in_hwy: &str, out_hwy: &str, next: (f64, f64), curr_is_intersection: bool) -> Graph {
        let nodes = vec![
            node(100, 0.0, 0.0, false),
            node(101, 0.0, 1.0, curr_is_intersection),
            node(102, next.0, next.1, false),
        ];
        let strings = vec![in_hwy.to_string(), out_hwy.to_string()];
        let edges = vec![edge(1, 0), edge(2, 1)]; // 0→1 (in_hwy), 1→2 (out_hwy)
        // CSR: node 0 owns edges[0..1], node 1 owns edges[1..2], node 2 none.
        let csr_offsets = vec![0, 1, 2, 2];
        Graph {
            heuristic_floor: 0.0,
            strings,
            nodes,
            edges,
            csr_offsets,
            segment_geometries: vec![],
        }
    }

    const LEFT: (f64, f64) = (-1.0, 1.0);
    const RIGHT: (f64, f64) = (1.0, 1.0);
    const STRAIGHT: (f64, f64) = (0.0, 2.0);

    /// Run compute_left_turn_penalty on a turn_graph. The outgoing edge
    /// (1→2) is edges[1].
    fn penalty(g: &Graph, is_intersection: bool) -> f32 {
        let next_edge = &g.edges[1];
        compute_left_turn_penalty(g, 0, 1, 2, next_edge, is_intersection)
    }

    // These tests assert *relationships* between penalties, never their
    // absolute magnitudes, so retuning LEFT_TURN_BASE_PENALTY or the
    // road_class_multiplier table won't break them. They fail only when
    // the *behavior* changes — the max logic regresses, the class
    // ordering inverts, or a gate stops firing. The two genuine zeros we
    // assert (gates, car-free turns) are structural "no penalty applies"
    // outcomes, not tuned values.

    // ---- calibration tripwire (the ONLY value-pinned test) ----

    #[test]
    fn calibration_snapshot() {
        // This is the one test that pins absolute values, on purpose. It
        // is NOT protecting a logical invariant — every other test does
        // that — it exists only to make an unintended change to the base
        // penalty or the multiplier table impossible to miss.
        //
        // When you retune deliberately, UPDATE these two numbers to match.
        // Exactly one test should go red per tuning pass; if anything else
        // breaks, you changed behavior, not just calibration.
        assert_eq!(LEFT_TURN_BASE_PENALTY, 50.0);
        assert_eq!(road_class_multiplier(Some("primary")), 3.5);
    }

    // ---- multiplier table shape (ordering, not magnitudes) ----

    #[test]
    fn multiplier_ordering_is_monotonic() {
        // The intended stress ranking of the main road classes. Guards the
        // table's shape without pinning any value; inverting it (a real
        // semantic change) is what should trip this.
        let m = |h| road_class_multiplier(Some(h));
        assert!(m("primary") > m("secondary"));
        assert!(m("secondary") > m("tertiary"));
        assert!(m("tertiary") > m("residential"));
        assert!(m("residential") > m("service"));
        assert!(m("service") > m("cycleway"));
    }

    #[test]
    fn car_free_classes_have_no_multiplier() {
        // Design decision: turning onto/off a path imposes no left-turn
        // stress. This is a structural zero, not a tuning knob.
        assert_eq!(road_class_multiplier(Some("cycleway")), 0.0);
        assert_eq!(road_class_multiplier(Some("path")), 0.0);
        assert_eq!(road_class_multiplier(Some("pedestrian")), 0.0);
    }

    #[test]
    fn unknown_highway_uses_default() {
        assert_eq!(road_class_multiplier(Some("motorway")), DEFAULT_ROAD_MULTIPLIER);
        assert_eq!(road_class_multiplier(None), DEFAULT_ROAD_MULTIPLIER);
    }

    // ---- penalty logic (relationships, tuning-proof) ----

    #[test]
    fn penalty_is_symmetric_in_road_class() {
        // Turning off A onto B costs the same as off B onto A — a direct
        // consequence of max() being symmetric. Immune to *all* retuning.
        let sym = |a, b| {
            penalty(&turn_graph(a, b, LEFT, true), true)
                == penalty(&turn_graph(b, a, LEFT, true), true)
        };
        assert!(sym("primary", "path"));
        assert!(sym("secondary", "residential"));
        assert!(sym("residential", "cycleway"));
    }

    #[test]
    fn penalty_is_governed_by_the_higher_class() {
        // The core of the change *and* the regression guard: a left off a
        // primary onto a path is NOT cheap — it costs the same as a left
        // between two primaries, because the primary governs either way.
        let p = |a, b| penalty(&turn_graph(a, b, LEFT, true), true);
        assert_eq!(p("primary", "path"), p("primary", "primary"));
        assert_eq!(p("primary", "path"), p("path", "primary"));
    }

    #[test]
    fn penalty_increases_with_the_involved_road_class() {
        // Holding the turn fixed, a left involving a higher class costs
        // strictly more. Survives base retuning; survives multiplier
        // retuning as long as the ordering above is preserved.
        let p = |a, b| penalty(&turn_graph(a, b, LEFT, true), true);
        let primary = p("primary", "residential");
        let secondary = p("secondary", "residential");
        let tertiary = p("tertiary", "residential");
        let quiet = p("residential", "residential");
        assert!(primary > secondary);
        assert!(secondary > tertiary);
        assert!(tertiary > quiet);
        assert!(quiet > 0.0);
    }

    // ---- gates and structural zeros ----

    #[test]
    fn car_free_left_is_free() {
        // Both classes have multiplier 0, so max() is 0 → no penalty.
        let g = turn_graph("cycleway", "path", LEFT, true);
        assert_eq!(penalty(&g, true), 0.0);
    }

    #[test]
    fn no_penalty_when_not_intersection() {
        let g = turn_graph("primary", "primary", LEFT, false);
        assert_eq!(penalty(&g, false), 0.0);
    }

    #[test]
    fn no_penalty_on_right_turn() {
        let g = turn_graph("primary", "primary", RIGHT, true);
        assert_eq!(penalty(&g, true), 0.0);
    }

    #[test]
    fn no_penalty_when_going_straight() {
        let g = turn_graph("primary", "primary", STRAIGHT, true);
        assert_eq!(penalty(&g, true), 0.0);
    }
}
