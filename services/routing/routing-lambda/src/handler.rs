//! Request handler: warming-ping detection, API key check, request
//! body parse, find_route, response shaping.
//!
//! Mirrors the existing Python `lambda_handler` behavior. The response
//! shape is identical so the frontend and route logger see no change.

use std::time::Instant;

use routing_core::{find_route, route_result_to_json, FindRouteError};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info};

use crate::error::HandlerError;
use crate::loader;

/// Top-level event handler. Always returns a JSON value with
/// `statusCode`, `headers`, `body` — never errors back to lambda_runtime
/// (an Err there would be a function invocation error, not what API
/// Gateway wants).
pub async fn handle(event: Value) -> Value {
    // Warming ping: EventBridge invokes the Lambda directly with this
    // payload. The handler skips request processing but the loader
    // still runs (it's a no-op on warm starts), keeping the graph
    // in memory.
    if event.get("source").and_then(Value::as_str) == Some("warming-ping") {
        if let Err(e) = loader::eager_load().await {
            // A warming ping shouldn't fail loudly — log and return 200.
            error!(error = %e, "warming ping load failed");
        }
        info!("warming ping handled");
        return response(200, "warm");
    }

    match process_route_request(&event).await {
        Ok(value) => response_json(200, value),
        Err(e) => {
            let status = e.status_code();
            let message = e.body_message();
            if status >= 500 {
                error!(status, error = %e, "request failed");
            }
            response_json(status, json!({ "error": message }))
        }
    }
}

#[derive(Deserialize)]
struct RouteRequest {
    /// [lat, lon]. Validation that exactly 2 elements are present is
    /// handled at parse time via the LatLonPair newtype.
    origin: LatLonPair,
    destination: LatLonPair,
}

#[derive(Deserialize)]
#[serde(try_from = "Vec<f32>")]
struct LatLonPair {
    lat: f32,
    lon: f32,
}

impl TryFrom<Vec<f32>> for LatLonPair {
    type Error = String;
    fn try_from(v: Vec<f32>) -> Result<Self, Self::Error> {
        if v.len() != 2 {
            return Err(format!("expected 2 elements, got {}", v.len()));
        }
        Ok(LatLonPair { lat: v[0], lon: v[1] })
    }
}

async fn process_route_request(event: &Value) -> Result<Value, HandlerError> {
    // 1. Ensure graph + key are loaded. Caches after first call.
    let graph = loader::graph()
        .await
        .map_err(|e| HandlerError::ServiceUnavailable(e.to_string()))?;
    let api_key = loader::api_key()
        .await
        .map_err(|e| HandlerError::ServiceUnavailable(e.to_string()))?;

    // 2. API key check. API Gateway v2 with payload format 2.0 lowercases
    // all header names, so we look up "x-api-key" lowercase.
    let provided = event
        .get("headers")
        .and_then(|h| h.get("x-api-key"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if api_key.is_empty() || provided != api_key.as_str() {
        return Err(HandlerError::Unauthorized);
    }

    // 3. Parse request body. API Gateway delivers it as a JSON string
    // under "body" for HTTP API integrations.
    let body_str = event.get("body").and_then(Value::as_str).unwrap_or("{}");
    let req: RouteRequest = serde_json::from_str(body_str)
        .map_err(|e| HandlerError::BadRequest(format!("invalid body: {e}")))?;

    // 4. Route. Timing logged regardless of outcome so we can see how
    // long failures took to fail.
    let started = Instant::now();
    let result = find_route::find_route(
        graph,
        req.origin.lat,
        req.origin.lon,
        req.destination.lat,
        req.destination.lon,
    );
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;

    match result {
        Ok(r) => {
            info!(
                elapsed_ms,
                total_length_m = r.total_length_m,
                segments = r.segments.len(),
                "routed"
            );
            Ok(route_result_to_json(&r))
        }
        Err(FindRouteError::NoPath) => {
            info!(elapsed_ms, "no route found");
            Err(HandlerError::NoRoute(
                "no route found between origin and destination".to_string(),
            ))
        }
        Err(other) => {
            error!(elapsed_ms, error = %other, "routing failed");
            Err(HandlerError::Internal(other.to_string()))
        }
    }
}

/// Build an API Gateway v2 response with a plain string body.
fn response(status: u16, body: &str) -> Value {
    json!({
        "statusCode": status,
        "body": body,
    })
}

/// Build an API Gateway v2 response with a JSON body (the body is
/// serialized to a string, as required by the API Gateway proxy
/// integration format).
fn response_json(status: u16, body: Value) -> Value {
    json!({
        "statusCode": status,
        "headers": { "Content-Type": "application/json" },
        "body": serde_json::to_string(&body)
            .unwrap_or_else(|_| r#"{"error":"response serialization failed"}"#.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lat_lon_pair_rejects_wrong_length() {
        let r: Result<LatLonPair, _> = LatLonPair::try_from(vec![1.0]);
        assert!(r.is_err());
        let r: Result<LatLonPair, _> = LatLonPair::try_from(vec![1.0, 2.0, 3.0]);
        assert!(r.is_err());
    }

    #[test]
    fn lat_lon_pair_accepts_two_floats() {
        let p = LatLonPair::try_from(vec![41.8781, -87.6298]).unwrap();
        assert_eq!(p.lat, 41.8781);
        assert_eq!(p.lon, -87.6298);
    }

    #[test]
    fn route_request_parses_full_body() {
        let req: RouteRequest = serde_json::from_str(
            r#"{"origin": [41.88, -87.63], "destination": [41.89, -87.64]}"#,
        )
        .unwrap();
        assert_eq!(req.origin.lat, 41.88);
        assert_eq!(req.destination.lon, -87.64);
    }

    #[test]
    fn route_request_rejects_bad_shape() {
        let r: Result<RouteRequest, _> = serde_json::from_str(
            r#"{"origin": "not an array", "destination": [0, 0]}"#,
        );
        assert!(r.is_err());
    }

    #[test]
    fn handler_error_status_codes() {
        assert_eq!(HandlerError::BadRequest("x".into()).status_code(), 400);
        assert_eq!(HandlerError::Unauthorized.status_code(), 401);
        assert_eq!(HandlerError::NoRoute("x".into()).status_code(), 422);
        assert_eq!(HandlerError::Internal("x".into()).status_code(), 500);
        assert_eq!(HandlerError::ServiceUnavailable("x".into()).status_code(), 503);
    }

    #[test]
    fn internal_error_message_is_generic() {
        // We don't leak implementation details in 500 bodies.
        assert_eq!(
            HandlerError::Internal("kdtree panic at index 42".into()).body_message(),
            "Routing failed"
        );
    }
}
