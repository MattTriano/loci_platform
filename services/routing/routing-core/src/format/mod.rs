//! Binary graph format reader.
//!
//! See `docs/graph-format.md` for the on-disk format specification.
//! Constants in this module mirror that spec; any change here requires
//! a corresponding change in the doc and the Python writer.

mod reader;
mod types;

pub use reader::{read_from_bytes, read_from_path};
pub use types::{Edge, Graph, GraphFormatError, Node, SegmentGeometry};

/// Magic bytes identifying a routing graph file: ASCII "LOCI".
pub const MAGIC: [u8; 4] = *b"LOCI";

/// Current format version. Bump on any incompatible change.
pub const FORMAT_VERSION: u16 = 1;

/// Sentinel for nullable string indices.
pub const NULL_STR_IDX: u32 = u32::MAX;

/// Bit 0 of edge flags: forward orientation.
pub const EDGE_FLAG_FORWARD: u8 = 0b0000_0001;
