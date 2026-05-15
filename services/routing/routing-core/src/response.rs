//! JSON shaping for route responses.
//!
//! Centralized here so both `routing-lambda` (chunk 6) and
//! `routing-cli` (chunk 4) emit byte-identical responses. The shape
//! matches the existing Python Lambda's output exactly, so the
//! frontend (`apps/bike-map/index.html`) and the route logger don't
//! need to change.
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
//!           "length_m":               f32,
//!           "speed_factor":           f32 | null,
//!           "road_type_factor":       f32 | null,
//!           "infrastructure_factor":  f32 | null,
//!           "tunnel_factor":          f32 | null,
//!           "surface_factor":         f32 | null,
//!           "lighting_factor":        f32 | null,
//!           "crash_cost":             f32,
//!           "intersection_cost":      f32,
//!           "left_turn_penalty":      f32
//!         }
//!       },
//!       ...
//!     ]
//!   }

use serde_json::{json, Value};

use crate::find_route::{CostComponents, RouteResult, RouteSegment};

const COST_ROUND_DECIMALS: u32 = 4;
const LENGTH_ROUND_DECIMALS: u32 = 1;

/// Render a RouteResult as the JSON shape the frontend expects.
pub fn route_result_to_json(r: &RouteResult) -> Value {
    json!({
        "total_cost": round(r.total_cost, COST_ROUND_DECIMALS),
        "total_length_m": round(r.total_length_m, LENGTH_ROUND_DECIMALS),
        "nodes": r.nodes,
        "segments": r.segments.iter().map(segment_to_json).collect::<Vec<_>>(),
    })
}

fn segment_to_json(s: &RouteSegment) -> Value {
    json!({
        "coordinates": s.coordinates.iter().map(|(lon, lat)| json!([lon, lat])).collect::<Vec<_>>(),
        "length_m": round(s.length_m, LENGTH_ROUND_DECIMALS),
        "stress_cost": round(s.stress_cost, COST_ROUND_DECIMALS),
        "name": s.name,
        "highway": s.highway,
        "infra_type": s.infra_type,
        "cost_components": cost_components_to_json(&s.cost_components),
    })
}

fn cost_components_to_json(c: &CostComponents) -> Value {
    json!({
        "length_m": round(c.length_m, LENGTH_ROUND_DECIMALS),
        "speed_factor": c.speed_factor,
        "road_type_factor": c.road_type_factor,
        "infrastructure_factor": c.infrastructure_factor,
        "tunnel_factor": c.tunnel_factor,
        "surface_factor": c.surface_factor,
        "lighting_factor": c.lighting_factor,
        "crash_cost": round(c.crash_cost, COST_ROUND_DECIMALS),
        "intersection_cost": round(c.intersection_cost, COST_ROUND_DECIMALS),
        "left_turn_penalty": round(c.left_turn_penalty, COST_ROUND_DECIMALS),
    })
}

/// Round to N decimal places, matching the Python implementation's
/// `round(x, N)` calls in find_route. Returned as f64 because that's
/// what serde_json's Number can represent without precision loss after
/// the rounding multiply.
fn round(value: f32, decimals: u32) -> f64 {
    let factor = 10f64.powi(decimals as i32);
    (value as f64 * factor).round() / factor
}
