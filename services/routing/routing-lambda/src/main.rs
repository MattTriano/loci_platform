//! Bike-map routing Lambda — chunk 1 stub.
//!
//! Returns 503 for any real route request and a no-op success for
//! warming pings. Replaced by the real implementation in a later chunk
//! once routing-core is in place. The shape exists to verify the
//! build/deploy pipeline produces a valid Lambda zip and that the
//! function executes under the `provided.al2023` runtime on arm64.

use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    // Warming pings come straight from EventBridge with
    // `source == "warming-ping"`. Treat them as success so they don't
    // surface as errors in CloudWatch.
    if event.payload.get("source").and_then(Value::as_str) == Some("warming-ping") {
        return Ok(json!({ "statusCode": 200, "body": "warm (stub)" }));
    }

    Ok(json!({
        "statusCode": 503,
        "headers": { "Content-Type": "application/json" },
        "body": "{\"error\":\"Not deployed yet\"}"
    }))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::run(service_fn(handler)).await
}
