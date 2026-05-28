//! /loci_platform/services/routing/routing-core/src/format/mod.rs
//! Binary routing-graph format: shared constants, module wiring, and
//! public re-exports.
//!
//! See `docs/graph-format.md` for the authoritative byte-level spec.
//! The reader (`reader/`) and the Python writer
//! (`platform/airflow/dags/loci/exports/graph_format.py`) must agree on
//! everything in this module.

pub mod reader;
pub mod types;

pub use reader::{read_from_bytes, read_from_path};
pub use types::{Edge, Graph, GraphFormatError, Node, SegmentGeometry};

/// Magic bytes at the start of every graph file: b"LOCI".
pub const MAGIC: [u8; 4] = *b"LOCI";

/// Format version. Bumped to 2 for the f64-coordinate +
/// per-node is_intersection layout (was 1 for the f32 layout).
pub const FORMAT_VERSION: u16 = 2;

/// Sentinel string index meaning "no string" (SQL NULL). u32::MAX so it
/// can never collide with a real index into the string table.
pub const NULL_STR_IDX: u32 = u32::MAX;

/// Edge flag bit: set if the edge runs in the segment's canonical
/// (forward) direction. Clear means it's the reversed sibling, and the
/// stored geometry must be reversed when composing the response.
pub const EDGE_FLAG_FORWARD: u8 = 0b0000_0001;
