# /loci_platform/platform/tests/exports/test_graph_format.py
"""
Tests for the routing graph binary format writer.

These tests assert on the exact byte structure of the output, matching
the spec in services/routing/docs/graph-format.md. They are deliberately
*not* round-trip tests against the Rust reader, because a shared
misinterpretation of the spec by both writer and reader would round-trip
cleanly while still being wrong.

Format version: 2. Updates from v1: node coordinates are f64, each node
record is 32 bytes (with an is_intersection u8 and 7 bytes of padding),
geometry coordinates are f64.
"""

import io
import math
import struct

import pytest
from loci.exports.graph_format import (
    EDGE_FLAG_FORWARD,
    FORMAT_VERSION,
    MAGIC,
    NULL_STR_IDX,
    WriteEdge,
    WriteNode,
    WriteSegmentGeometry,
    f32_or_nan,
    write_graph,
)

# Layout constants from the spec — used to compute offsets in tests.
HEADER_SIZE = 16
NODE_RECORD_SIZE = 32
EDGE_RECORD_SIZE = 48
GEOM_COORD_SIZE = 16  # f64 lon + f64 lat


def _make_simple_writes():
    """A minimal valid graph: 3 nodes, 4 edges (one backward), 1 segment geom.

    Mirrors the Rust reader test fixture so byte-for-byte comparison
    against the Rust test would be possible if ever desired.
    """
    strings = ["S0", "S1", "S2", "Main St", "residential"]
    nodes = [
        WriteNode(osm_id=10001, lon=-87.6298, lat=41.8781, is_intersection=True),
        WriteNode(osm_id=10002, lon=-87.6299, lat=41.8782, is_intersection=False),
        WriteNode(osm_id=10003, lon=-87.6300, lat=41.8783, is_intersection=True),
    ]
    edges = [
        # source 0 → target 1, S0, named, residential, forward
        WriteEdge(
            source_node_idx=0,
            target_node_idx=1,
            segment_id_str_idx=0,
            name_str_idx=3,
            highway_str_idx=4,
            infra_type_str_idx=NULL_STR_IDX,
            forward=True,
            length_m=100.0,
            stress_cost=250.0,
            physical_cost=250.0,
            intersection_cost=0.0,
            crash_cost=0.0,
            elevation_cost=0.0,
        ),
        # source 1 → target 2, S1, unnamed, residential, forward
        WriteEdge(
            source_node_idx=1,
            target_node_idx=2,
            segment_id_str_idx=1,
            name_str_idx=NULL_STR_IDX,
            highway_str_idx=4,
            infra_type_str_idx=NULL_STR_IDX,
            forward=True,
            length_m=80.0,
            stress_cost=200.0,
            physical_cost=200.0,
            intersection_cost=0.0,
            crash_cost=0.0,
            elevation_cost=0.0,
        ),
        # source 1 → target 0, S0 backward sibling
        WriteEdge(
            source_node_idx=1,
            target_node_idx=0,
            segment_id_str_idx=0,
            name_str_idx=3,
            highway_str_idx=4,
            infra_type_str_idx=NULL_STR_IDX,
            forward=False,
            length_m=100.0,
            stress_cost=250.0,
            physical_cost=250.0,
            intersection_cost=0.0,
            crash_cost=0.0,
            elevation_cost=0.0,
        ),
        # source 2 → target 0, S2 forward
        WriteEdge(
            source_node_idx=2,
            target_node_idx=0,
            segment_id_str_idx=2,
            name_str_idx=NULL_STR_IDX,
            highway_str_idx=4,
            infra_type_str_idx=NULL_STR_IDX,
            forward=True,
            length_m=60.0,
            stress_cost=150.0,
            physical_cost=150.0,
            intersection_cost=0.0,
            crash_cost=0.0,
            elevation_cost=0.0,
        ),
    ]
    geoms = [
        WriteSegmentGeometry(
            segment_id_str_idx=0,
            coords=[(-87.6298, 41.8781), (-87.62985, 41.87815), (-87.6299, 41.8782)],
        ),
    ]
    return strings, nodes, edges, geoms


def _write_to_bytes(strings, nodes, edges, geoms, heuristic_floor=0.0001) -> bytes:
    buf = io.BytesIO()
    write_graph(
        buf,
        heuristic_floor=heuristic_floor,
        strings=strings,
        nodes=nodes,
        edges=edges,
        segment_geometries=geoms,
    )
    return buf.getvalue()


def test_header_layout():
    payload = _write_to_bytes(*_make_simple_writes())

    # Header is the first 16 bytes:
    assert payload[0:4] == MAGIC
    assert struct.unpack_from("<H", payload, 4)[0] == FORMAT_VERSION
    assert FORMAT_VERSION == 3  # Reminder: bump triggers reader update
    assert struct.unpack_from("<H", payload, 6)[0] == 0  # flags reserved
    assert struct.unpack_from("<d", payload, 8)[0] == pytest.approx(0.0001)


def test_string_table_layout():
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    # String table starts at offset 16. First u32 is the count.
    cursor = HEADER_SIZE
    count = struct.unpack_from("<I", payload, cursor)[0]
    cursor += 4
    assert count == len(strings)

    decoded = []
    for _ in range(count):
        length = struct.unpack_from("<I", payload, cursor)[0]
        cursor += 4
        decoded.append(payload[cursor : cursor + length].decode("utf-8"))
        cursor += length
    assert decoded == strings


def test_node_table_layout():
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    # Skip header + string table to locate the node table.
    cursor = HEADER_SIZE + _string_table_size(strings)

    count = struct.unpack_from("<I", payload, cursor)[0]
    cursor += 4
    assert count == len(nodes)

    for expected in nodes:
        record = payload[cursor : cursor + NODE_RECORD_SIZE]
        osm_id = struct.unpack_from("<Q", record, 0)[0]
        lon = struct.unpack_from("<d", record, 8)[0]
        lat = struct.unpack_from("<d", record, 16)[0]
        is_intersection_byte = record[24]
        padding = record[25:32]

        assert osm_id == expected.osm_id
        # f64 precision — exact comparison ok for these values.
        assert lon == pytest.approx(expected.lon)
        assert lat == pytest.approx(expected.lat)
        assert is_intersection_byte in (0, 1)
        assert bool(is_intersection_byte) == expected.is_intersection
        assert padding == b"\x00" * 7, "node padding must be zero"
        cursor += NODE_RECORD_SIZE


def test_edge_table_records_are_48_bytes():
    """Edge record shrank to 44 bytes in v2 and grew to 48 in v3."""
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    cursor = HEADER_SIZE + _string_table_size(strings) + 4 + len(nodes) * NODE_RECORD_SIZE

    count = struct.unpack_from("<I", payload, cursor)[0]
    cursor += 4
    assert count == len(edges)

    for expected in edges:
        record = payload[cursor : cursor + EDGE_RECORD_SIZE]
        target = struct.unpack_from("<I", record, 0)[0]
        seg = struct.unpack_from("<I", record, 4)[0]
        name = struct.unpack_from("<I", record, 8)[0]
        hwy = struct.unpack_from("<I", record, 12)[0]
        infra = struct.unpack_from("<I", record, 16)[0]
        flags = record[20]
        padding = record[21:24]
        length = struct.unpack_from("<f", record, 24)[0]
        stress = struct.unpack_from("<f", record, 28)[0]
        physical = struct.unpack_from("<f", record, 32)[0]
        intersection = struct.unpack_from("<f", record, 36)[0]
        crash = struct.unpack_from("<f", record, 40)[0]
        elevation = struct.unpack_from("<f", record, 44)[0]

        assert padding == b"\x00\x00\x00", "edge padding must be zero"
        assert flags & 0b1111_1110 == 0, "reserved edge flag bits must be zero"
        assert target == expected.target_node_idx
        assert seg == expected.segment_id_str_idx
        assert name == expected.name_str_idx
        assert hwy == expected.highway_str_idx
        assert infra == expected.infra_type_str_idx
        assert bool(flags & EDGE_FLAG_FORWARD) == expected.forward
        assert length == pytest.approx(expected.length_m, rel=1e-5)
        assert stress == pytest.approx(expected.stress_cost, rel=1e-5)
        assert physical == pytest.approx(expected.physical_cost, rel=1e-5)
        assert intersection == pytest.approx(expected.intersection_cost, rel=1e-5)
        assert crash == pytest.approx(expected.crash_cost, rel=1e-5)
        assert elevation == pytest.approx(expected.elevation_cost, rel=1e-5)
        cursor += EDGE_RECORD_SIZE


def test_csr_offsets_match_edge_source_distribution():
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    cursor = (
        HEADER_SIZE
        + _string_table_size(strings)
        + 4
        + len(nodes) * NODE_RECORD_SIZE
        + 4
        + len(edges) * EDGE_RECORD_SIZE
    )

    count = struct.unpack_from("<I", payload, cursor)[0]
    cursor += 4
    assert count == len(nodes) + 1

    offsets = []
    for _ in range(count):
        offsets.append(struct.unpack_from("<I", payload, cursor)[0])
        cursor += 4

    # Our fixture: 1 edge from node 0, 2 from node 1, 1 from node 2.
    # Offsets are cumulative starts.
    assert offsets == [0, 1, 3, 4]
    # Final offset always equals edge count.
    assert offsets[-1] == len(edges)


def test_segment_geometry_layout():
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    cursor = (
        HEADER_SIZE
        + _string_table_size(strings)
        + 4
        + len(nodes) * NODE_RECORD_SIZE
        + 4
        + len(edges) * EDGE_RECORD_SIZE
        + 4
        + (len(nodes) + 1) * 4
    )

    count = struct.unpack_from("<I", payload, cursor)[0]
    cursor += 4
    assert count == len(geoms)

    for expected in geoms:
        seg_idx = struct.unpack_from("<I", payload, cursor)[0]
        cursor += 4
        coord_count = struct.unpack_from("<I", payload, cursor)[0]
        cursor += 4
        assert seg_idx == expected.segment_id_str_idx
        assert coord_count == len(expected.coords)
        for expected_lon, expected_lat in expected.coords:
            lon = struct.unpack_from("<d", payload, cursor)[0]
            lat = struct.unpack_from("<d", payload, cursor + 8)[0]
            cursor += GEOM_COORD_SIZE
            # f64 has plenty of precision for these test values.
            assert lon == pytest.approx(expected_lon)
            assert lat == pytest.approx(expected_lat)

    # And nothing trailing.
    assert cursor == len(payload)


def test_unordered_edges_raise_value_error():
    """The writer enforces source-node-idx ordering at write time."""
    strings, nodes, edges, geoms = _make_simple_writes()
    # Reverse the edges so they're no longer sorted by source.
    reversed_edges = list(reversed(edges))
    with pytest.raises(ValueError, match="not sorted"):
        _write_to_bytes(strings, nodes, reversed_edges, geoms)


def test_empty_graph_writes_minimal_payload():
    """No nodes, no edges, no geometries; just header + empty sections."""
    payload = _write_to_bytes(
        strings=[],
        nodes=[],
        edges=[],
        geoms=[],
        heuristic_floor=0.0,
    )
    # Header (16) + 4 (strings count=0) + 4 (nodes count=0)
    #  + 4 (edges count=0) + 4 (csr count=1) + 4 (csr entry=0)
    #  + 4 (geoms count=0)
    assert len(payload) == 16 + 4 + 4 + 4 + 4 + 4 + 4
    # CSR for zero nodes is still one entry (count = node_count + 1 = 1).
    csr_count_offset = 16 + 4 + 4 + 4
    assert struct.unpack_from("<I", payload, csr_count_offset)[0] == 1


def test_node_padding_bytes_are_zero():
    """Explicit check that node padding is exactly seven zero bytes."""
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    cursor = HEADER_SIZE + _string_table_size(strings) + 4  # past node count
    for _ in range(len(nodes)):
        padding = payload[cursor + 25 : cursor + 32]
        assert padding == b"\x00" * 7
        cursor += NODE_RECORD_SIZE


def test_is_intersection_byte_round_trips():
    """is_intersection=True writes 1, =False writes 0."""
    nodes = [
        WriteNode(osm_id=1, lon=0.0, lat=0.0, is_intersection=False),
        WriteNode(osm_id=2, lon=0.0, lat=0.0, is_intersection=True),
        WriteNode(osm_id=3, lon=0.0, lat=0.0, is_intersection=False),
    ]
    payload = _write_to_bytes(
        strings=[],
        nodes=nodes,
        edges=[],
        geoms=[],
    )
    cursor = HEADER_SIZE + 4 + 4  # header + empty string table count + node count
    flags = [payload[cursor + i * NODE_RECORD_SIZE + 24] for i in range(3)]
    assert flags == [0, 1, 0]


def test_f64_coordinate_precision_preserved():
    """f64 coordinates round-trip without the precision loss f32 had."""
    # A coordinate that f32 can't represent exactly but f64 can.
    high_precision_lon = -87.62983456789012
    high_precision_lat = 41.87815432109876
    nodes = [
        WriteNode(
            osm_id=1,
            lon=high_precision_lon,
            lat=high_precision_lat,
            is_intersection=False,
        ),
    ]
    payload = _write_to_bytes(strings=[], nodes=nodes, edges=[], geoms=[])

    cursor = HEADER_SIZE + 4 + 4  # past string-table count and node count
    lon = struct.unpack_from("<d", payload, cursor + 8)[0]
    lat = struct.unpack_from("<d", payload, cursor + 16)[0]
    # Exact equality — f64 holds these without loss.
    assert lon == high_precision_lon
    assert lat == high_precision_lat


def test_f32_or_nan_handles_none():
    assert math.isnan(f32_or_nan(None))
    assert f32_or_nan(1.5) == 1.5
    assert f32_or_nan(0) == 0.0


def _string_table_size(strings) -> int:
    """Total bytes consumed by the string table for given strings list."""
    return 4 + sum(4 + len(s.encode("utf-8")) for s in strings)
