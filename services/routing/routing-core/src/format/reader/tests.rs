//! Reader tests with a hand-rolled byte fixture.
//!
//! The fixture is built byte-by-byte rather than via a sibling writer.
//! This is deliberate: if a future writer (chunk 5) and this reader
//! both share a misinterpretation of the spec, a round-trip test
//! would pass and the spec-vs-implementation drift would go unnoticed.
//! Hand-rolled bytes anchor the implementation to `docs/graph-format.md`.

use super::*;
use crate::format::{
    types::GraphFormatError, EDGE_FLAG_FORWARD, FORMAT_VERSION, MAGIC, NULL_STR_IDX,
};

/// Build a minimal valid fixture: 3 nodes, 4 edges, 1 segment geometry.
///
/// Layout:
///   N0 ── e0 ──▶ N1    (segment S0, forward, with geometry)
///   N1 ── e1 ──▶ N2    (segment S1, forward, no geometry)
///   N2 ── e2 ──▶ N0    (segment S2, forward, no geometry)
///   N1 ── e3 ──▶ N0    (segment S0, backward, geometry shared with e0)
///
/// Strings table:
///   [0] = "S0"           (segment id)
///   [1] = "S1"
///   [2] = "S2"
///   [3] = "Main St"      (name on e0/e3)
///   [4] = "residential"  (highway)
fn build_fixture() -> Vec<u8> {
    let mut b = Vec::new();

    // --- Header ---
    b.extend_from_slice(&MAGIC);
    b.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    b.extend_from_slice(&0u16.to_le_bytes()); // flags reserved
    b.extend_from_slice(&0.0001_f64.to_le_bytes()); // heuristic_floor

    // --- String table ---
    let strings = ["S0", "S1", "S2", "Main St", "residential"];
    b.extend_from_slice(&(strings.len() as u32).to_le_bytes());
    for s in strings {
        b.extend_from_slice(&(s.len() as u32).to_le_bytes());
        b.extend_from_slice(s.as_bytes());
    }

    // --- Node table ---
    let nodes: [(u64, f32, f32); 3] = [
        (10001, -87.6298, 41.8781), // Chicago-ish
        (10002, -87.6299, 41.8782),
        (10003, -87.6300, 41.8783),
    ];
    b.extend_from_slice(&(nodes.len() as u32).to_le_bytes());
    for (osm, lon, lat) in nodes {
        b.extend_from_slice(&osm.to_le_bytes());
        b.extend_from_slice(&lon.to_le_bytes());
        b.extend_from_slice(&lat.to_le_bytes());
    }

    // --- Edge table (CSR-ordered by source node) ---
    // source 0: e0 (target 1, seg "S0", forward, named)
    // source 1: e1 (target 2, seg "S1"), e3 (target 0, seg "S0", backward)
    // source 2: e2 (target 0, seg "S2")
    let edges = [
        // (target, seg_idx, name_idx, hwy_idx, infra_idx, flags, length, stress)
        (
            1u32,
            0u32,
            3u32,
            4u32,
            NULL_STR_IDX,
            EDGE_FLAG_FORWARD,
            100.0f32,
            250.0f32,
        ),
        (
            2,
            1,
            NULL_STR_IDX,
            4,
            NULL_STR_IDX,
            EDGE_FLAG_FORWARD,
            80.0,
            200.0,
        ),
        (0, 0, 3, 4, NULL_STR_IDX, 0, 100.0, 250.0), // backward sibling of e0
        (
            0,
            2,
            NULL_STR_IDX,
            4,
            NULL_STR_IDX,
            EDGE_FLAG_FORWARD,
            60.0,
            150.0,
        ),
    ];
    // CSR order: source 0 → e0; source 1 → e1, e3 (backward sibling); source 2 → e2
    // Edge index within file: 0=e0, 1=e1, 2=e3-backward, 3=e2
    b.extend_from_slice(&(edges.len() as u32).to_le_bytes());
    for (target, seg, name, hwy, infra, flags, length, stress) in edges {
        b.extend_from_slice(&target.to_le_bytes());
        b.extend_from_slice(&seg.to_le_bytes());
        b.extend_from_slice(&name.to_le_bytes());
        b.extend_from_slice(&hwy.to_le_bytes());
        b.extend_from_slice(&infra.to_le_bytes());
        b.push(flags);
        b.extend_from_slice(&[0, 0, 0]); // padding
        b.extend_from_slice(&length.to_le_bytes());
        b.extend_from_slice(&stress.to_le_bytes());
        // 9 remaining f32 cost-component fields, all NaN to exercise the null path
        for _ in 0..9 {
            b.extend_from_slice(&f32::NAN.to_le_bytes());
        }
    }

    // --- CSR offsets ---
    // node 0 has 1 outgoing (e0); node 1 has 2 (e1, e3-backward); node 2 has 1 (e2)
    let offsets: [u32; 4] = [0, 1, 3, 4];
    b.extend_from_slice(&(offsets.len() as u32).to_le_bytes());
    for o in offsets {
        b.extend_from_slice(&o.to_le_bytes());
    }

    // --- Segment geometry table ---
    // Only S0 has geometry: 3 coords
    let geom = [(
        0u32,
        vec![
            (-87.6298_f32, 41.8781_f32),
            (-87.62985, 41.87815),
            (-87.6299, 41.8782),
        ],
    )];
    b.extend_from_slice(&(geom.len() as u32).to_le_bytes());
    for (seg_idx, coords) in &geom {
        b.extend_from_slice(&seg_idx.to_le_bytes());
        b.extend_from_slice(&(coords.len() as u32).to_le_bytes());
        for (lon, lat) in coords {
            b.extend_from_slice(&lon.to_le_bytes());
            b.extend_from_slice(&lat.to_le_bytes());
        }
    }

    b
}

#[test]
fn reads_valid_fixture() {
    let bytes = build_fixture();
    let g = read_from_bytes(&bytes).expect("fixture should parse");

    // Header
    assert!((g.heuristic_floor - 0.0001).abs() < 1e-9);

    // Strings
    assert_eq!(g.strings, vec!["S0", "S1", "S2", "Main St", "residential"]);

    // Nodes
    assert_eq!(g.nodes.len(), 3);
    assert_eq!(g.nodes[0].osm_id, 10001);
    assert!((g.nodes[0].lon - -87.6298).abs() < 1e-4);

    // Edges
    assert_eq!(g.edges.len(), 4);
    assert!(g.edges[0].is_forward());
    assert!(!g.edges[2].is_forward());
    assert_eq!(g.edges[0].length_m, 100.0);
    assert!(g.edges[0].speed_factor.is_nan());

    // Nullability
    assert_eq!(g.lookup_str(g.edges[0].name_str_idx), Some("Main St"));
    assert_eq!(g.lookup_str(g.edges[1].name_str_idx), None);

    // CSR
    assert_eq!(g.csr_offsets, vec![0, 1, 3, 4]);
    assert_eq!(g.edges_from(0).len(), 1);
    assert_eq!(g.edges_from(1).len(), 2);
    assert_eq!(g.edges_from(2).len(), 1);

    // Geometry
    assert_eq!(g.segment_geometries.len(), 1);
    assert_eq!(g.segment_geometries[0].coords.len(), 3);
}

#[test]
fn rejects_bad_magic() {
    let mut bytes = build_fixture();
    bytes[0] = b'X';
    match read_from_bytes(&bytes) {
        Err(GraphFormatError::BadMagic { .. }) => {}
        other => panic!("expected BadMagic, got {other:?}"),
    }
}

#[test]
fn rejects_unsupported_version() {
    let mut bytes = build_fixture();
    // version field is at offset 4, 2 bytes LE
    bytes[4] = 99;
    bytes[5] = 0;
    match read_from_bytes(&bytes) {
        Err(GraphFormatError::UnsupportedVersion { got: 99, .. }) => {}
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
}

#[test]
fn rejects_reserved_flags() {
    let mut bytes = build_fixture();
    // flags field is at offset 6, 2 bytes LE
    bytes[6] = 1;
    match read_from_bytes(&bytes) {
        Err(GraphFormatError::ReservedNotZero { field, .. }) if field.contains("flags") => {}
        other => panic!("expected ReservedNotZero(header flags), got {other:?}"),
    }
}

#[test]
fn rejects_trailing_bytes() {
    let mut bytes = build_fixture();
    bytes.push(0xAA);
    match read_from_bytes(&bytes) {
        Err(GraphFormatError::TrailingBytes { count: 1 }) => {}
        other => panic!("expected TrailingBytes(1), got {other:?}"),
    }
}

#[test]
fn rejects_truncated_file() {
    let bytes = build_fixture();
    let truncated = &bytes[..bytes.len() - 4];
    match read_from_bytes(truncated) {
        Err(GraphFormatError::UnexpectedEof { .. }) => {}
        other => panic!("expected UnexpectedEof, got {other:?}"),
    }
}

#[test]
fn rejects_csr_offsets_with_wrong_count() {
    // Build a fixture where the CSR offset count is wrong.
    // Cheapest way: edit the CSR count field directly. It lives after
    // the edge table; rather than computing offsets here, rebuild
    // the fixture and corrupt the byte we know.
    let mut bytes = build_fixture();
    // Find the CSR offset count: scan for the value 4 (node_count+1) preceded by edges.
    // Simpler: replicate the layout math from build_fixture above to locate the byte
    // window. Given the fixture is fixed, the CSR count u32 lives at a known offset.
    //
    // header(16) + string_table + node_table(4 + 3*16) + edge_table(4 + 4*68)
    //   = 16 + string_size + 52 + 276
    let string_size = string_table_size_of(&["S0", "S1", "S2", "Main St", "residential"]);
    let csr_count_offset = 16 + string_size + 52 + 276;
    // Confirm what we expect to see there (count = 4, i.e. node_count + 1).
    let actual = u32::from_le_bytes([
        bytes[csr_count_offset],
        bytes[csr_count_offset + 1],
        bytes[csr_count_offset + 2],
        bytes[csr_count_offset + 3],
    ]);
    assert_eq!(
        actual, 4,
        "fixture layout drift: csr count not at expected offset"
    );

    // Corrupt it.
    bytes[csr_count_offset..csr_count_offset + 4].copy_from_slice(&5u32.to_le_bytes());
    // We also need to provide an extra u32 in the offsets so the reader doesn't
    // hit EOF instead of CsrOffsetCountMismatch. Insert a zero offset.
    bytes.splice(
        csr_count_offset + 4 + 16..csr_count_offset + 4 + 16,
        [0u8; 4],
    );

    match read_from_bytes(&bytes) {
        Err(GraphFormatError::CsrOffsetCountMismatch {
            expected: 4,
            got: 5,
        }) => {}
        other => panic!("expected CsrOffsetCountMismatch, got {other:?}"),
    }
}

fn string_table_size_of(strings: &[&str]) -> usize {
    // count u32 + per-string (u32 len + bytes)
    4 + strings.iter().map(|s| 4 + s.len()).sum::<usize>()
}
