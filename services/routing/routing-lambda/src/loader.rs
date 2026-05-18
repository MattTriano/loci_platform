//! Cold-start loader for the routing graph and the API key.
//!
//! Both load on first access and cache for the lifetime of the Lambda
//! execution environment. The Python implementation does this in
//! `_ensure_loaded`; here we use `tokio::sync::OnceCell` for the same
//! effect with proper async support.

use std::env;
use std::io::Read;

use aws_config::BehaviorVersion;
use flate2::read::GzDecoder;
use routing_core::{format::read_from_bytes, IndexedGraph};
use thiserror::Error;
use tokio::sync::OnceCell;
use tracing::{info, warn};

/// Loaded routing graph. Populated on first access from S3.
static GRAPH: OnceCell<IndexedGraph> = OnceCell::const_new();

/// API key. Populated on first access from SSM (or env var fallback).
static API_KEY: OnceCell<String> = OnceCell::const_new();

#[derive(Error, Debug)]
pub enum LoadError {
    #[error("missing required env var: {0}")]
    MissingEnv(&'static str),

    #[error("s3 get_object failed: {0}")]
    S3GetObject(String),

    #[error("s3 body read failed: {0}")]
    S3BodyRead(String),

    #[error("gzip decompress failed: {0}")]
    Decompress(String),

    #[error("graph parse failed: {0}")]
    GraphParse(String),

    #[error("ssm get_parameter failed: {0}")]
    SsmGetParameter(String),

    #[error("ssm parameter has no value")]
    SsmNoValue,
}

/// Load both graph and API key. Called on cold start.
pub async fn eager_load() -> Result<(), LoadError> {
    graph().await?;
    api_key().await?;
    Ok(())
}

/// Get the loaded graph, loading on first call. Subsequent calls
/// return the cached value without I/O.
pub async fn graph() -> Result<&'static IndexedGraph, LoadError> {
    GRAPH.get_or_try_init(load_graph).await
}

/// Get the loaded API key, loading on first call.
pub async fn api_key() -> Result<&'static String, LoadError> {
    API_KEY.get_or_try_init(load_api_key).await
}

async fn load_graph() -> Result<IndexedGraph, LoadError> {
    let bucket = env::var("BIKE_MAP_GRAPH_BUCKET")
        .map_err(|_| LoadError::MissingEnv("BIKE_MAP_GRAPH_BUCKET"))?;
    let key =
        env::var("BIKE_MAP_GRAPH_KEY").map_err(|_| LoadError::MissingEnv("BIKE_MAP_GRAPH_KEY"))?;
    info!(%bucket, %key, "loading graph from S3");

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);

    let resp = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .map_err(|e| LoadError::S3GetObject(e.to_string()))?;

    let body = resp
        .body
        .collect()
        .await
        .map_err(|e| LoadError::S3BodyRead(e.to_string()))?
        .into_bytes();
    info!(compressed_bytes = body.len(), "graph fetched, decompressing");

    let mut decoder = GzDecoder::new(&body[..]);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| LoadError::Decompress(e.to_string()))?;
    info!(decompressed_bytes = decompressed.len(), "graph decompressed");

    let graph = read_from_bytes(&decompressed)
        .map_err(|e| LoadError::GraphParse(e.to_string()))?;

    let indexed = IndexedGraph::build(graph);
    info!(
        nodes = indexed.graph.nodes.len(),
        edges = indexed.graph.edges.len(),
        "graph indexed and ready"
    );
    Ok(indexed)
}

async fn load_api_key() -> Result<String, LoadError> {
    // Match the Python behavior: prefer SSM, fall back to env var.
    if let Ok(ssm_name) = env::var("BIKE_MAP_API_KEY_SSM_ARN") {
        if !ssm_name.is_empty() {
            info!(parameter = %ssm_name, "loading API key from SSM");
            let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let client = aws_sdk_ssm::Client::new(&config);

            let resp = client
                .get_parameter()
                .name(ssm_name)
                .with_decryption(true)
                .send()
                .await
                .map_err(|e| LoadError::SsmGetParameter(e.to_string()))?;

            return resp
                .parameter
                .and_then(|p| p.value)
                .ok_or(LoadError::SsmNoValue);
        }
    }
    warn!("BIKE_MAP_API_KEY_SSM_ARN not set; falling back to BIKE_MAP_API_KEY env var");
    Ok(env::var("BIKE_MAP_API_KEY").unwrap_or_default())
}
