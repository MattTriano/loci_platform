//! /loci_platform/services/routing/routing-core/src/format/reader/mod.rs
//! Reader for the binary routing graph format.
//!
//! Operates on an in-memory byte slice — the caller decompresses gzip
//! data first (or uses `read_from_path`, which handles that).

use std::fs::File;
use std::io::Read;
use std::path::Path;

use flate2::read::GzDecoder;

use super::types::{Edge, Graph, GraphFormatError, Node, SegmentGeometry};
use super::{EDGE_FLAG_FORWARD, FORMAT_VERSION, MAGIC, NULL_STR_IDX};

/// Read a gzip-compressed routing graph file from disk.
///
/// Convenience wrapper around `read_from_bytes` that handles file IO
/// and gzip decompression.
pub fn read_from_path(path: &Path) -> Result<Graph, GraphFormatError> {
    let file = File::open(path)?;
    let mut decoder = GzDecoder::new(file);
    let mut buf = Vec::new();
    decoder.read_to_end(&mut buf)?;
    read_from_bytes(&buf)
}

/// Read a routing graph from a decompressed byte buffer.
///
/// The buffer must contain the format payload (post-gzip-decompression).
pub fn read_from_bytes(bytes: &[u8]) -> Result<Graph, GraphFormatError> {
    let mut cursor = Cursor::new(bytes);

    // --- Header ---
    let magic = cursor.read_array::<4>("header magic")?;
    if magic != MAGIC {
        return Err(GraphFormatError::BadMagic {
            expected: MAGIC,
            got: magic,
        });
    }
    let version = cursor.read_u16("header version")?;
    if version != FORMAT_VERSION {
        return Err(GraphFormatError::UnsupportedVersion {
            got: version,
            supported: FORMAT_VERSION,
        });
    }
    let flags = cursor.read_u16("header flags")?;
    if flags != 0 {
        return Err(GraphFormatError::ReservedNotZero {
            field: "header flags",
            value: u64::from(flags),
        });
    }
    let heuristic_floor = cursor.read_f64("heuristic_floor")?;

    // --- String table ---
    let string_count = cursor.read_u32("string table count")? as usize;
    let mut strings: Vec<String> = Vec::with_capacity(string_count);
    for idx in 0..string_count {
        let len = cursor.read_u32("string length")? as usize;
        let bytes = cursor.read_slice(len, "string bytes")?;
        let s = std::str::from_utf8(bytes)
            .map_err(|_| GraphFormatError::InvalidUtf8 { idx })?
            .to_owned();
        strings.push(s);
    }

    // --- Node table ---
    // Each node record is 32 bytes: u64 osm_id + f64 lon + f64 lat
    // + u8 is_intersection + 7 bytes zero padding.
    let node_count = cursor.read_u32("node table count")? as usize;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        let osm_id = cursor.read_u64("node.osm_id")?;
        let lon = cursor.read_f64("node.lon")?;
        let lat = cursor.read_f64("node.lat")?;
        let is_intersection = cursor.read_bool("node.is_intersection")?;
        let padding = cursor.read_array::<7>("node.padding")?;
        if padding != [0u8; 7] {
            return Err(GraphFormatError::ReservedNotZero {
                field: "node padding bytes",
                value: pack_padding(&padding),
            });
        }
        nodes.push(Node {
            osm_id,
            lon,
            lat,
            is_intersection,
        });
    }

    // --- Edge table ---
    let edge_count = cursor.read_u32("edge table count")?;
    let mut edges = Vec::with_capacity(edge_count as usize);
    for _ in 0..edge_count {
        edges.push(read_edge(&mut cursor, strings.len())?);
    }

    // --- CSR offsets ---
    let csr_offset_count = cursor.read_u32("csr offset count")? as usize;
    let expected = node_count + 1;
    if csr_offset_count != expected {
        return Err(GraphFormatError::CsrOffsetCountMismatch {
            expected,
            got: csr_offset_count,
        });
    }
    let mut csr_offsets: Vec<u32> = Vec::with_capacity(csr_offset_count);
    for _ in 0..csr_offset_count {
        csr_offsets.push(cursor.read_u32("csr offset")?);
    }
    validate_csr_offsets(&csr_offsets, edge_count)?;

    // --- Segment geometry table ---
    // Coordinates are f64 (format v2).
    let geom_count = cursor.read_u32("segment geometry count")?;
    let mut segment_geometries = Vec::with_capacity(geom_count as usize);
    for _ in 0..geom_count {
        let segment_id_str_idx = cursor.read_u32("geom segment_id_str_idx")?;
        check_str_idx(segment_id_str_idx, strings.len(), false)?;
        let coord_count = cursor.read_u32("geom coord_count")? as usize;
        let mut coords = Vec::with_capacity(coord_count);
        for _ in 0..coord_count {
            let lon = cursor.read_f64("geom lon")?;
            let lat = cursor.read_f64("geom lat")?;
            coords.push((lon, lat));
        }
        segment_geometries.push(SegmentGeometry {
            segment_id_str_idx,
            coords,
        });
    }

    // --- Trailing-byte check ---
    let remaining = cursor.remaining();
    if remaining > 0 {
        return Err(GraphFormatError::TrailingBytes { count: remaining });
    }

    Ok(Graph {
        heuristic_floor,
        strings,
        nodes,
        edges,
        csr_offsets,
        segment_geometries,
    })
}

/// Pack up to 8 padding bytes into a u64 for diagnostics.
fn pack_padding(padding: &[u8]) -> u64 {
    padding.iter().fold(0u64, |acc, &b| (acc << 8) | u64::from(b))
}

fn read_edge(cursor: &mut Cursor<'_>, str_table_len: usize) -> Result<Edge, GraphFormatError> {
    let target_node_idx = cursor.read_u32("edge.target")?;
    let segment_id_str_idx = cursor.read_u32("edge.segment_id")?;
    check_str_idx(segment_id_str_idx, str_table_len, false)?;
    let name_str_idx = cursor.read_u32("edge.name")?;
    check_str_idx(name_str_idx, str_table_len, true)?;
    let highway_str_idx = cursor.read_u32("edge.highway")?;
    check_str_idx(highway_str_idx, str_table_len, true)?;
    let infra_type_str_idx = cursor.read_u32("edge.infra_type")?;
    check_str_idx(infra_type_str_idx, str_table_len, true)?;

    let flags = cursor.read_u8("edge.flags")?;
    if flags & !EDGE_FLAG_FORWARD != 0 {
        return Err(GraphFormatError::ReservedNotZero {
            field: "edge.flags reserved bits",
            value: u64::from(flags),
        });
    }
    let padding = cursor.read_array::<3>("edge.padding")?;
    if padding != [0, 0, 0] {
        return Err(GraphFormatError::ReservedNotZero {
            field: "edge padding bytes",
            value: pack_padding(&padding),
        });
    }

    Ok(Edge {
        target_node_idx,
        segment_id_str_idx,
        name_str_idx,
        highway_str_idx,
        infra_type_str_idx,
        flags,
        length_m: cursor.read_f32("edge.length_m")?,
        stress_cost: cursor.read_f32("edge.stress_cost")?,
        physical_cost: cursor.read_f32("edge.physical_cost")?,
        intersection_cost: cursor.read_f32("edge.intersection_cost")?,
        crash_cost: cursor.read_f32("edge.crash_cost")?,
        elevation_cost: cursor.read_f32("edge.elevation_cost")?,
    })
}

fn check_str_idx(idx: u32, table_len: usize, nullable: bool) -> Result<(), GraphFormatError> {
    if nullable && idx == NULL_STR_IDX {
        return Ok(());
    }
    if (idx as usize) >= table_len {
        return Err(GraphFormatError::StringIndexOutOfBounds {
            idx,
            len: table_len,
        });
    }
    Ok(())
}

fn validate_csr_offsets(offsets: &[u32], edge_count: u32) -> Result<(), GraphFormatError> {
    for (i, window) in offsets.windows(2).enumerate() {
        let (prev, next) = (window[0], window[1]);
        if next < prev {
            return Err(GraphFormatError::CsrOffsetsNotMonotonic { pos: i, prev, next });
        }
    }
    if let Some(&final_offset) = offsets.last() {
        if final_offset != edge_count {
            return Err(GraphFormatError::CsrFinalOffsetMismatch {
                final_offset,
                edge_count,
            });
        }
    }
    Ok(())
}

// ----------------------------------------------------------------------
// Cursor: small helper for byte-by-byte reads with named-section errors.
// ----------------------------------------------------------------------

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn read_slice(
        &mut self,
        n: usize,
        section: &'static str,
    ) -> Result<&'a [u8], GraphFormatError> {
        if self.remaining() < n {
            return Err(GraphFormatError::UnexpectedEof { section });
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_array<const N: usize>(
        &mut self,
        section: &'static str,
    ) -> Result<[u8; N], GraphFormatError> {
        let slice = self.read_slice(N, section)?;
        let mut arr = [0u8; N];
        arr.copy_from_slice(slice);
        Ok(arr)
    }

    fn read_u8(&mut self, section: &'static str) -> Result<u8, GraphFormatError> {
        Ok(self.read_array::<1>(section)?[0])
    }

    /// Read a u8 constrained to 0 or 1. Any other value is a format error.
    fn read_bool(&mut self, section: &'static str) -> Result<bool, GraphFormatError> {
        match self.read_u8(section)? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(GraphFormatError::InvalidBool {
                field: section,
                value: other,
            }),
        }
    }

    fn read_u16(&mut self, section: &'static str) -> Result<u16, GraphFormatError> {
        Ok(u16::from_le_bytes(self.read_array(section)?))
    }

    fn read_u32(&mut self, section: &'static str) -> Result<u32, GraphFormatError> {
        Ok(u32::from_le_bytes(self.read_array(section)?))
    }

    fn read_u64(&mut self, section: &'static str) -> Result<u64, GraphFormatError> {
        Ok(u64::from_le_bytes(self.read_array(section)?))
    }

    fn read_f32(&mut self, section: &'static str) -> Result<f32, GraphFormatError> {
        Ok(f32::from_le_bytes(self.read_array(section)?))
    }

    fn read_f64(&mut self, section: &'static str) -> Result<f64, GraphFormatError> {
        Ok(f64::from_le_bytes(self.read_array(section)?))
    }
}

#[cfg(test)]
mod tests;
