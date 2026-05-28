//! /loci_platform/services/routing/routing-core/src/response.rs
//! JSON shaping for route responses.
//!
//! Centralized here so both `routing-lambda` and `routing-cli` emit
//! byte-identical responses. The shape matches the existing Python
//! Lambda's output, so the frontend (`apps/bike-map/index.html`) and
//! the route logger don't need to change.
//!
//! Response shape:
//!   {
//!     "total_cost":      f32,
//!     "total_length_m":  f32,
//!     "nodes":           [osm_id, ...],
//!     "segments": [
//!       {
//!         "coordinates":  [[lon, lat], ...],
//!         "length_m":     f32,
//!         "stress_cost":  f32,
//!         "name":         "..." | null,
//!         "highway":      "..." | null,
//!         "infra_type":   "..." | null,
//!         "cost_components": {
//!           "length_m":          f32,
//!           "physical_cost":     f32,
//!           "crash_cost":        f32,
//!           "intersection_cost": f32,
//!           "left_turn_penalty": f32
//!         }
//!       },
//!       ...
//!     ]
//!   }

use serde_json::{json, Value};

use crate::find_route::{CostComponents, RouteResult, RouteSegment};

const COST_ROUND_DECIMALS: u32 = 4;
const LENGTH_ROUND_DECIMALS: u32 = 1;
// Coordinates are f64 in the graph now. 6 decimal places is ~0.11 m at
// the equator — far finer than bike routing needs — and keeps the JSON
// payload from ballooning with spurious f64 precision.
const COORD_ROUND_DECIMALS: u32 = 6;

/// Render a RouteResult as the JSON shape the frontend expects.
pub fn route_result_to_json(r: &RouteResult) -> Value {
    json!({
        "total_cost": round_f32(r.total_cost, COST_ROUND_DECIMALS),
        "total_length_m": round_f32(r.total_length_m, LENGTH_ROUND_DECIMALS),
        "nodes": r.nodes,
        "segments": r.segments.iter().map(segment_to_json).collect::<Vec<_>>(),
    })
}

fn segment_to_json(s: &RouteSegment) -> Value {
    json!({
        "coordinates": s
            .coordinates
            .iter()
            .map(|(lon, lat)| json!([round_coord(*lon), round_coord(*lat)]))
            .collect::<Vec<_>>(),
        "length_m": round_f32(s.length_m, LENGTH_ROUND_DECIMALS),
        "stress_cost": round_f32(s.stress_cost, COST_ROUND_DECIMALS),
        "name": s.name,
        "highway": s.highway,
        "infra_type": s.infra_type,
        "cost_components": cost_components_to_json(&s.cost_components),
    })
}

fn cost_components_to_json(c: &CostComponents) -> Value {
    json!({
        "length_m": round_f32(c.length_m, LENGTH_ROUND_DECIMALS),
        "physical_cost": round_f32(c.physical_cost, COST_ROUND_DECIMALS),
        "crash_cost": round_f32(c.crash_cost, COST_ROUND_DECIMALS),
        "intersection_cost": round_f32(c.intersection_cost, COST_ROUND_DECIMALS),
        "left_turn_penalty": round_f32(c.left_turn_penalty, COST_ROUND_DECIMALS),
    })
}

/// Round an f32 to N decimal places. Returned as f64 because that's what
/// serde_json's Number can represent without precision artifacts after
/// the rounding multiply.
fn round_f32(value: f32, decimals: u32) -> f64 {
    round_f64(value as f64, decimals)
}

/// Round an f64 coordinate to N decimal places.
fn round_coord(value: f64) -> f64 {
    round_f64(value, COORD_ROUND_DECIMALS)
}

fn round_f64(value: f64, decimals: u32) -> f64 {
    let factor = 10f64.powi(decimals as i32);
    (value * factor).round() / factor
}
