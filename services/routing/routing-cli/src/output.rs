//! JSON output shaping for the CLI.
//!
//! Converts `routing_core::RouteResult` into a `serde_json::Value`
//! whose shape matches the existing Lambda's response. Centralized
//! here so the Lambda binary (chunk 6) can reuse the same shaper.
//!
//! Response shape (matches what apps/bike-map/index.html expects):
//!   {
//!     "total_cost": f32,
//!     "total_length_m": f32,
//!     "nodes": [osm_id, ...],
//!     "segments": [
//!       {
//!         "coordinates": [[lon, lat], ...],
//!         "length_m": f32,
//!         "stress_cost": f32,
//!         "name": "..." | null,
//!         "highway": "..." | null,
//!         "infra_type": "..." | null,
//!         "cost_components": {
//!           "length_m": f32,
//!           "speed_factor": f32 | null,
//!           "road_type_factor": f32 | null,
//!           "infrastructure_factor": f32 | null,
//!           "tunnel_factor": f32 | null,
//!           "surface_factor": f32 | null,
//!           "lighting_factor": f32 | null,
//!           "crash_cost": f32,
//!           "intersection_cost": f32,
//!           "left_turn_penalty": f32
//!         }
//!       },
//!       ...
//!     ]
//!   }

use routing_core::{CostComponents, RouteResult, RouteSegment};
use serde_json::{json, Value};

const COST_ROUND_DECIMALS: u32 = 4;
const LENGTH_ROUND_DECIMALS: u32 = 1;

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
/// `round(x, N)` calls in find_route. Keeps the JSON output stable
/// across A/B comparisons with the existing Lambda.
fn round(value: f32, decimals: u32) -> f64 {
    let factor = 10f64.powi(decimals as i32);
    (value as f64 * factor).round() / factor
}
