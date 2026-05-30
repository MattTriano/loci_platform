//! /loci_platform/services/routing/routing-core/src/format/types.rs
//! Data structures for a loaded routing graph.
//!
//! The layout mirrors the on-disk format: a CSR-style edge list with
//! source nodes implicit in `csr_offsets`. Strings are deduplicated
//! into `strings`; references elsewhere are u32 indices into it
//! (or NULL_STR_IDX for missing values).

use std::io;

use thiserror::Error;

/// A single node in the routing graph.
///
/// Coordinates are f64 (format v2). `is_intersection` is the
/// authoritative flag for turn-penalty logic, set by the data pipeline
/// rather than re-derived from edge degree at load time.
#[derive(Debug, Clone, Copy)]
pub struct Node {
    pub osm_id: u64,
    pub lon: f64,
    pub lat: f64,
    pub is_intersection: bool,
}

/// A single directed edge. Source node is implicit in `csr_offsets`.
///
/// `stress_cost` is the per-direction routing weight; `physical_cost`,
/// `intersection_cost`, and `crash_cost` are its components, kept for
/// explaining a segment's cost. f32 fields use NaN for SQL NULL.
#[derive(Debug, Clone, Copy)]
pub struct Edge {
    pub target_node_idx: u32,
    pub segment_id_str_idx: u32,
    pub name_str_idx: u32,
    pub highway_str_idx: u32,
    pub infra_type_str_idx: u32,
    pub flags: u8,
    pub length_m: f32,
    pub stress_cost: f32,
    pub physical_cost: f32,
    pub intersection_cost: f32,
    pub crash_cost: f32,
}

impl Edge {
    /// True if this edge has the `forward` orientation flag set.
    /// Used by the response composer to decide whether segment geometry
    /// needs reversing.
    pub fn is_forward(&self) -> bool {
        self.flags & super::EDGE_FLAG_FORWARD != 0
    }
}

/// Geometry for a single segment. Edges referencing the same segment
/// share this geometry; orientation is resolved at response time.
///
/// Coordinates are f64 (format v2).
#[derive(Debug, Clone)]
pub struct SegmentGeometry {
    pub segment_id_str_idx: u32,
    /// (lon, lat) pairs in segment-canonical order.
    pub coords: Vec<(f64, f64)>,
}

/// A fully loaded routing graph.
///
/// Edges are CSR-ordered: `edges[csr_offsets[n]..csr_offsets[n+1]]` is
/// the slice of edges leaving node `n`. `csr_offsets.len() == nodes.len() + 1`.
#[derive(Debug)]
pub struct Graph {
    pub heuristic_floor: f64,
    pub strings: Vec<String>,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub csr_offsets: Vec<u32>,
    pub segment_geometries: Vec<SegmentGeometry>,
}

impl Graph {
    /// Resolve a string index, returning `None` for NULL_STR_IDX.
    /// Returns `Err`-like None also for indices that overflow the table,
    /// though such a graph is malformed and should be caught at load time.
    pub fn lookup_str(&self, idx: u32) -> Option<&str> {
        if idx == super::NULL_STR_IDX {
            return None;
        }
        self.strings.get(idx as usize).map(String::as_str)
    }

    /// Slice of edges leaving node `n`.
    pub fn edges_from(&self, n: u32) -> &[Edge] {
        let start = self.csr_offsets[n as usize] as usize;
        let end = self.csr_offsets[n as usize + 1] as usize;
        &self.edges[start..end]
    }
}

/// Errors raised when reading a routing graph file.
#[derive(Error, Debug)]
pub enum GraphFormatError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error(
        "magic bytes did not match: expected {expected:?}, got {got:?} \
         (is this a routing graph file?)"
    )]
    BadMagic { expected: [u8; 4], got: [u8; 4] },

    #[error("unsupported format version {got}, this binary understands version {supported}")]
    UnsupportedVersion { got: u16, supported: u16 },

    #[error("reserved field had non-zero value: {field} = {value}")]
    ReservedNotZero { field: &'static str, value: u64 },

    #[error("invalid boolean for {field}: {value} (expected 0 or 1)")]
    InvalidBool { field: &'static str, value: u8 },

    #[error("string index {idx} out of bounds for string table of length {len}")]
    StringIndexOutOfBounds { idx: u32, len: usize },

    #[error("CSR offsets malformed: expected {expected} entries, got {got}")]
    CsrOffsetCountMismatch { expected: usize, got: usize },

    #[error("CSR offsets not monotonic at position {pos}: {prev} -> {next}")]
    CsrOffsetsNotMonotonic { pos: usize, prev: u32, next: u32 },

    #[error("final CSR offset {final_offset} does not match edge count {edge_count}")]
    CsrFinalOffsetMismatch { final_offset: u32, edge_count: u32 },

    #[error("unexpected trailing bytes ({count}) after end of geometry table")]
    TrailingBytes { count: usize },

    #[error("unexpected end of file while reading {section}")]
    UnexpectedEof { section: &'static str },

    #[error("invalid UTF-8 in string table at index {idx}")]
    InvalidUtf8 { idx: usize },
}
