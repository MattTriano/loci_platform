//! /loci_platform/services/routing/routing-core/src/lib.rs
//! routing-core: types and pure routing logic for bike-map.
//!
//! Chunks 2-4 added: binary graph format, KD-tree, A*, turn penalties,
//! find_route. Chunk 6 moves the JSON response shaper here so the
//! Lambda and CLI binaries share a single source of truth.

pub mod astar;
pub mod find_route;
pub mod format;
pub mod geom;
pub mod indexed;
pub mod kdtree;
pub mod response;
pub mod turn_cost;

pub use find_route::{
    find_route as run_find_route, CostComponents, FindRouteError, RouteResult, RouteSegment,
};
pub use format::{Edge, Graph, GraphFormatError, Node, SegmentGeometry};
pub use indexed::IndexedGraph;
pub use response::route_result_to_json;
