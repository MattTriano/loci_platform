//! routing-core: types and pure routing logic for bike-map.
//!
//! Chunk 3 adds the routing algorithm: KD-tree indexed graph,
//! turn-aware A*, and the public `find_route` entry point.

pub mod astar;
pub mod find_route;
pub mod format;
pub mod geom;
pub mod indexed;
pub mod kdtree;
pub mod turn_cost;

pub use find_route::{
    find_route as run_find_route, CostComponents, FindRouteError, RouteResult, RouteSegment,
};
pub use format::{Edge, Graph, GraphFormatError, Node, SegmentGeometry};
pub use indexed::IndexedGraph;
