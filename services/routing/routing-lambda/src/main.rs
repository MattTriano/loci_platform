//! Bike-map routing Lambda — entry point.
//!
//! Initializes tracing for CloudWatch, eagerly loads the routing graph
//! and API key, and hands the request handler to lambda_runtime.

mod error;
mod handler;
mod loader;

use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use tracing::info;

async fn dispatch(event: LambdaEvent<Value>) -> Result<Value, Error> {
    // Convert any error into a structured response shape. Returning Err
    // from a handler surfaces as a Lambda invocation error, which is
    // not what API Gateway wants — we always want a status-coded JSON
    // body.
    Ok(handler::handle(event.payload).await)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // CloudWatch picks up the default format. RUST_LOG controls the
    // level at deploy time via Lambda environment variables.
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    // Eagerly load on cold start. This pulls the graph from S3 and the
    // API key from SSM before the first invocation, so the first real
    // request doesn't pay the load cost. Failures here will cause the
    // Lambda init to fail; AWS retries cold start on next invocation.
    if let Err(e) = loader::eager_load().await {
        // Don't abort: log and let the first request return a 503.
        // This matches the Python behavior (which returns 503 if
        // _ensure_loaded raises).
        tracing::error!(error = %e, "eager load failed; first request will retry");
    } else {
        info!("graph and API key loaded on cold start");
    }

    lambda_runtime::run(service_fn(dispatch)).await
}
