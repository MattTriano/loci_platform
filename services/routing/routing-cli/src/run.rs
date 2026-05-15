//! Graph loading and route execution.

use std::io::{self, BufRead, Write};
use std::path::Path;

use routing_core::{
    find_route::find_route as core_find_route, format::read_from_path, route_result_to_json,
    FindRouteError, IndexedGraph,
};
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RunError {
    #[error("no route found")]
    NoRoute,
    #[error("routing failed: {0}")]
    Internal(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

pub fn load_graph(path: &Path) -> Result<IndexedGraph, String> {
    let graph = read_from_path(path).map_err(|e| format!("{e}"))?;
    Ok(IndexedGraph::build(graph))
}

pub fn run_single(
    g: &IndexedGraph,
    origin_lat: f32,
    origin_lon: f32,
    dest_lat: f32,
    dest_lon: f32,
) -> Result<String, RunError> {
    let result =
        core_find_route(g, origin_lat, origin_lon, dest_lat, dest_lon).map_err(|e| match e {
            FindRouteError::NoPath => RunError::NoRoute,
            other => RunError::Internal(other.to_string()),
        })?;
    let value = route_result_to_json(&result);
    serde_json::to_string(&value).map_err(|e| RunError::Internal(e.to_string()))
}

/// Run a batch of route requests from stdin, write results to stdout.
///
/// Each input line is a JSON object with at minimum:
///   { "origin": [lat, lon], "destination": [lat, lon] }
///
/// Any other fields on the input line are echoed back on the output
/// line under "request", so test harnesses can match results to
/// fixtures (e.g. by a test name field).
///
/// Each output line is one JSON object:
///   - On success: {"status": "ok", "request": <input>, "result": <route>}
///   - On no-route: {"status": "no_route", "request": <input>}
///   - On bad input: {"status": "bad_request", "error": "..."} (no
///     "request" field if the input wasn't valid JSON).
///
/// Returns the number of non-success outcomes so the CLI can pick its
/// exit code: 0 if all green, EXIT_FIXTURES_HAD_FAILURES otherwise.
pub fn run_fixtures(g: &IndexedGraph) -> Result<u32, RunError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut failures = 0u32;

    for (idx, line) in stdin.lock().lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let result = process_fixture_line(g, &line, idx + 1);
        if result.get("status").and_then(Value::as_str) != Some("ok") {
            failures += 1;
        }
        writeln!(out, "{result}")?;
    }

    Ok(failures)
}

fn process_fixture_line(g: &IndexedGraph, line: &str, line_number: usize) -> Value {
    let request: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return json!({
                "status": "bad_request",
                "error": format!("invalid JSON at line {line_number}: {e}"),
            });
        }
    };

    let (origin_lat, origin_lon) = match extract_latlon(&request, "origin") {
        Ok(p) => p,
        Err(msg) => {
            return json!({
                "status": "bad_request",
                "request": request,
                "error": msg,
            });
        }
    };
    let (dest_lat, dest_lon) = match extract_latlon(&request, "destination") {
        Ok(p) => p,
        Err(msg) => {
            return json!({
                "status": "bad_request",
                "request": request,
                "error": msg,
            });
        }
    };

    match core_find_route(g, origin_lat, origin_lon, dest_lat, dest_lon) {
        Ok(r) => json!({
            "status": "ok",
            "request": request,
            "result": route_result_to_json(&r),
        }),
        Err(FindRouteError::NoPath) => json!({
            "status": "no_route",
            "request": request,
        }),
        Err(e) => json!({
            "status": "error",
            "request": request,
            "error": e.to_string(),
        }),
    }
}

fn extract_latlon(req: &Value, field: &str) -> Result<(f32, f32), String> {
    let arr = req
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("missing or non-array field {field:?}"))?;
    if arr.len() != 2 {
        return Err(format!(
            "{field:?} must have exactly 2 elements, got {}",
            arr.len()
        ));
    }
    let lat = arr[0]
        .as_f64()
        .ok_or_else(|| format!("{field:?}[0] must be a number"))? as f32;
    let lon = arr[1]
        .as_f64()
        .ok_or_else(|| format!("{field:?}[1] must be a number"))? as f32;
    Ok((lat, lon))
}
