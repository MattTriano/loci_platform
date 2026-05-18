//! End-to-end CLI tests.
//!
//! Each test builds a small graph file on disk (using the chunk 2
//! binary format spec directly), gzips it, runs the CLI binary
//! against it via assert_cmd, and asserts on the JSON output.
//!
//! These tests are the first end-to-end exercise of:
//!   - The chunk 2 reader (loading the file)
//!   - The chunk 3 indexing + A* + response composition
//!   - The chunk 4 CLI argument parsing and JSON shaping
//! all in one place, against the same byte-level format the Python
//! writer (chunk 5) will eventually produce.

use std::io::Write;

use assert_cmd::Command;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::Value;
use tempfile::tempdir;

const MAGIC: &[u8; 4] = b"LOCI";
const VERSION: u16 = 1;
const FLAGS: u16 = 0;
const EDGE_FLAG_FORWARD: u8 = 0b0000_0001;
const NULL_STR_IDX: u32 = u32::MAX;

/// Construct a minimal 3-node, 2-edge linear graph at three points
/// near Chicago. Output is the uncompressed byte payload; caller
/// gzips and writes to disk.
fn build_simple_chain_bytes() -> Vec<u8> {
    let mut b = Vec::new();

    // Header
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&VERSION.to_le_bytes());
    b.extend_from_slice(&FLAGS.to_le_bytes());
    b.extend_from_slice(&0.0001_f64.to_le_bytes());

    // String table: ["seg_a", "seg_b", "Main St", "residential"]
    let strings = ["seg_a", "seg_b", "Main St", "residential"];
    b.extend_from_slice(&(strings.len() as u32).to_le_bytes());
    for s in strings {
        b.extend_from_slice(&(s.len() as u32).to_le_bytes());
        b.extend_from_slice(s.as_bytes());
    }

    // Nodes
    let nodes: [(u64, f32, f32); 3] = [
        (10001, -87.6298, 41.8781),
        (10002, -87.6298, 41.8791), // ~111m N of N0
        (10003, -87.6298, 41.8801), // ~111m N of N1
    ];
    b.extend_from_slice(&(nodes.len() as u32).to_le_bytes());
    for (osm, lon, lat) in nodes {
        b.extend_from_slice(&osm.to_le_bytes());
        b.extend_from_slice(&lon.to_le_bytes());
        b.extend_from_slice(&lat.to_le_bytes());
    }

    // Edges: 0 → 1 (seg_a, named, residential), 1 → 2 (seg_b, residential)
    let edges = [
        (
            1u32,
            0u32,
            2u32,
            3u32,
            NULL_STR_IDX,
            EDGE_FLAG_FORWARD,
            111.0f32,
            222.0f32,
        ),
        (
            2,
            1,
            NULL_STR_IDX,
            3,
            NULL_STR_IDX,
            EDGE_FLAG_FORWARD,
            111.0,
            222.0,
        ),
    ];
    b.extend_from_slice(&(edges.len() as u32).to_le_bytes());
    for (target, seg, name, hwy, infra, flags, length, stress) in edges {
        b.extend_from_slice(&target.to_le_bytes());
        b.extend_from_slice(&seg.to_le_bytes());
        b.extend_from_slice(&name.to_le_bytes());
        b.extend_from_slice(&hwy.to_le_bytes());
        b.extend_from_slice(&infra.to_le_bytes());
        b.push(flags);
        b.extend_from_slice(&[0, 0, 0]);
        b.extend_from_slice(&length.to_le_bytes());
        b.extend_from_slice(&stress.to_le_bytes());
        for _ in 0..9 {
            b.extend_from_slice(&f32::NAN.to_le_bytes());
        }
    }

    // CSR offsets: node 0 → 1 edge, node 1 → 1 edge, node 2 → 0 edges
    let offsets: [u32; 4] = [0, 1, 2, 2];
    b.extend_from_slice(&(offsets.len() as u32).to_le_bytes());
    for o in offsets {
        b.extend_from_slice(&o.to_le_bytes());
    }

    // Segment geometry: seg_a (2 coords), seg_b (2 coords)
    let geom: [(u32, &[(f32, f32)]); 2] = [
        (0, &[(-87.6298, 41.8781), (-87.6298, 41.8791)]),
        (1, &[(-87.6298, 41.8791), (-87.6298, 41.8801)]),
    ];
    b.extend_from_slice(&(geom.len() as u32).to_le_bytes());
    for (seg_idx, coords) in geom {
        b.extend_from_slice(&seg_idx.to_le_bytes());
        b.extend_from_slice(&(coords.len() as u32).to_le_bytes());
        for (lon, lat) in coords {
            b.extend_from_slice(&lon.to_le_bytes());
            b.extend_from_slice(&lat.to_le_bytes());
        }
    }

    b
}

fn write_graph_file(bytes: &[u8], path: &std::path::Path) {
    let f = std::fs::File::create(path).expect("create graph file");
    let mut enc = GzEncoder::new(f, Compression::default());
    enc.write_all(bytes).expect("write graph bytes");
    enc.finish().expect("finalize gzip");
}

#[test]
fn single_route_returns_expected_shape() {
    let dir = tempdir().unwrap();
    let graph = dir.path().join("graph.bin.gz");
    write_graph_file(&build_simple_chain_bytes(), &graph);

    let assert = Command::cargo_bin("routing-cli")
        .unwrap()
        .args([
            "--graph",
            graph.to_str().unwrap(),
            "--origin",
            "41.8781,-87.6298",
            "--destination",
            "41.8801,-87.6298",
        ])
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    let v: Value = serde_json::from_str(stdout.trim()).expect("stdout should be JSON");

    // Top-level shape
    assert!(v.get("total_cost").and_then(Value::as_f64).is_some());
    assert!(v.get("total_length_m").and_then(Value::as_f64).is_some());
    assert_eq!(v["nodes"].as_array().unwrap().len(), 3);
    assert_eq!(v["nodes"][0], 10001);
    assert_eq!(v["nodes"][2], 10003);

    // Segments
    let segs = v["segments"].as_array().unwrap();
    assert_eq!(segs.len(), 2);

    let s0 = &segs[0];
    assert_eq!(s0["name"], "Main St");
    assert_eq!(s0["highway"], "residential");
    assert_eq!(s0["infra_type"], Value::Null);
    let coords = s0["coordinates"].as_array().unwrap();
    assert_eq!(coords.len(), 2);
    // [lon, lat] order
    assert!((coords[0][0].as_f64().unwrap() + 87.6298).abs() < 1e-3);
    assert!((coords[0][1].as_f64().unwrap() - 41.8781).abs() < 1e-3);

    let cc = &s0["cost_components"];
    assert_eq!(cc["speed_factor"], Value::Null);
    assert!(cc.get("left_turn_penalty").is_some());
}

#[test]
fn coincident_origin_destination_returns_single_node() {
    let dir = tempdir().unwrap();
    let graph = dir.path().join("graph.bin.gz");
    write_graph_file(&build_simple_chain_bytes(), &graph);

    let assert = Command::cargo_bin("routing-cli")
        .unwrap()
        .args([
            "--graph",
            graph.to_str().unwrap(),
            "--origin",
            "41.8781,-87.6298",
            "--destination",
            "41.8781,-87.6298",
        ])
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    let v: Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(v["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(v["segments"].as_array().unwrap().len(), 0);
    assert_eq!(v["total_length_m"], 0.0);
}

#[test]
fn missing_graph_file_exits_with_load_error() {
    Command::cargo_bin("routing-cli")
        .unwrap()
        .args([
            "--graph",
            "/nonexistent/path/graph.bin.gz",
            "--origin",
            "0,0",
            "--destination",
            "0,0",
        ])
        .assert()
        .failure()
        .code(2); // EXIT_GRAPH_LOAD
}

#[test]
fn missing_origin_destination_with_no_fixtures_is_bad_args() {
    let dir = tempdir().unwrap();
    let graph = dir.path().join("graph.bin.gz");
    write_graph_file(&build_simple_chain_bytes(), &graph);

    Command::cargo_bin("routing-cli")
        .unwrap()
        .args(["--graph", graph.to_str().unwrap()])
        .assert()
        .failure()
        .code(1); // EXIT_BAD_ARGS
}

#[test]
fn fixtures_mode_processes_jsonl() {
    let dir = tempdir().unwrap();
    let graph = dir.path().join("graph.bin.gz");
    write_graph_file(&build_simple_chain_bytes(), &graph);

    let input = concat!(
        r#"{"test_name": "valid_route", "origin": [41.8781, -87.6298], "destination": [41.8801, -87.6298]}"#,
        "\n",
        r#"{"test_name": "coincident", "origin": [41.8781, -87.6298], "destination": [41.8781, -87.6298]}"#,
        "\n",
        r#"{"test_name": "missing_destination", "origin": [41.8781, -87.6298]}"#,
        "\n",
    );

    let assert = Command::cargo_bin("routing-cli")
        .unwrap()
        .args(["--graph", graph.to_str().unwrap(), "--fixtures"])
        .write_stdin(input)
        .assert()
        .failure() // because the third line is bad
        .code(5); // EXIT_FIXTURES_HAD_FAILURES

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3);

    let l0: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(l0["status"], "ok");
    assert_eq!(l0["request"]["test_name"], "valid_route");
    assert!(l0["result"]["segments"].as_array().unwrap().len() == 2);

    let l1: Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(l1["status"], "ok");
    assert_eq!(l1["request"]["test_name"], "coincident");

    let l2: Value = serde_json::from_str(lines[2]).unwrap();
    assert_eq!(l2["status"], "bad_request");
    assert!(l2["error"].as_str().unwrap().contains("destination"));
}

#[test]
fn fixtures_mode_all_ok_returns_zero() {
    let dir = tempdir().unwrap();
    let graph = dir.path().join("graph.bin.gz");
    write_graph_file(&build_simple_chain_bytes(), &graph);

    let input = r#"{"origin": [41.8781, -87.6298], "destination": [41.8801, -87.6298]}"#;

    Command::cargo_bin("routing-cli")
        .unwrap()
        .args(["--graph", graph.to_str().unwrap(), "--fixtures"])
        .write_stdin(input)
        .assert()
        .success()
        .code(0); // EXIT_OK
}
