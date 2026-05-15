"""
Tests for the routing graph binary format writer.

These tests assert on the exact byte structure of the output, matching
the spec in services/routing/docs/graph-format.md. They are deliberately
*not* round-trip tests against the Rust reader, because a shared
misinterpretation of the spec by both writer and reader would round-trip
cleanly while still being wrong.
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


def _make_simple_writes():
    """A minimal valid graph: 3 nodes, 4 edges (one backward), 1 segment geom.

    Mirrors the chunk 2 Rust fixture shape so byte-for-byte comparison
    against the Rust test would be possible if ever desired.
    """
    strings = ["S0", "S1", "S2", "Main St", "residential"]
    nodes = [
        WriteNode(osm_id=10001, lon=-87.6298, lat=41.8781),
        WriteNode(osm_id=10002, lon=-87.6299, lat=41.8782),
        WriteNode(osm_id=10003, lon=-87.6300, lat=41.8783),
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
            speed_factor=math.nan,
            road_type_factor=math.nan,
            infrastructure_factor=math.nan,
            tunnel_factor=math.nan,
            surface_factor=math.nan,
            lighting_factor=math.nan,
            physical_cost=math.nan,
            intersection_cost=math.nan,
            crash_cost=math.nan,
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
            speed_factor=math.nan,
            road_type_factor=math.nan,
            infrastructure_factor=math.nan,
            tunnel_factor=math.nan,
            surface_factor=math.nan,
            lighting_factor=math.nan,
            physical_cost=math.nan,
            intersection_cost=math.nan,
            crash_cost=math.nan,
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
            speed_factor=math.nan,
            road_type_factor=math.nan,
            infrastructure_factor=math.nan,
            tunnel_factor=math.nan,
            surface_factor=math.nan,
            lighting_factor=math.nan,
            physical_cost=math.nan,
            intersection_cost=math.nan,
            crash_cost=math.nan,
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
            speed_factor=math.nan,
            road_type_factor=math.nan,
            infrastructure_factor=math.nan,
            tunnel_factor=math.nan,
            surface_factor=math.nan,
            lighting_factor=math.nan,
            physical_cost=math.nan,
            intersection_cost=math.nan,
            crash_cost=math.nan,
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
    assert struct.unpack_from("<H", payload, 6)[0] == 0  # flags reserved
    assert struct.unpack_from("<d", payload, 8)[0] == pytest.approx(0.0001)


def test_string_table_layout():
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    # String table starts at offset 16. First u32 is the count.
    cursor = 16
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
    cursor = _string_table_size(strings) + 16

    count = struct.unpack_from("<I", payload, cursor)[0]
    cursor += 4
    assert count == len(nodes)

    for expected in nodes:
        osm_id = struct.unpack_from("<Q", payload, cursor)[0]
        lon = struct.unpack_from("<f", payload, cursor + 8)[0]
        lat = struct.unpack_from("<f", payload, cursor + 12)[0]
        cursor += 16
        assert osm_id == expected.osm_id
        assert lon == pytest.approx(expected.lon, rel=1e-5)
        assert lat == pytest.approx(expected.lat, rel=1e-5)


def test_edge_table_records_are_68_bytes():
    """The spec says 68 bytes/edge after the 'wait that's 68 not 56' fix."""
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    cursor = _string_table_size(strings) + 16 + 4 + len(nodes) * 16

    count = struct.unpack_from("<I", payload, cursor)[0]
    cursor += 4
    assert count == len(edges)

    for expected in edges:
        record = payload[cursor : cursor + 68]
        target = struct.unpack_from("<I", record, 0)[0]
        seg = struct.unpack_from("<I", record, 4)[0]
        name = struct.unpack_from("<I", record, 8)[0]
        hwy = struct.unpack_from("<I", record, 12)[0]
        infra = struct.unpack_from("<I", record, 16)[0]
        flags = record[20]
        padding = record[21:24]
        length = struct.unpack_from("<f", record, 24)[0]
        stress = struct.unpack_from("<f", record, 28)[0]

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
        cursor += 68


def test_csr_offsets_match_edge_source_distribution():
    strings, nodes, edges, geoms = _make_simple_writes()
    payload = _write_to_bytes(strings, nodes, edges, geoms)

    cursor = _string_table_size(strings) + 16 + 4 + len(nodes) * 16
    cursor += 4 + len(edges) * 68

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

    cursor = _string_table_size(strings) + 16 + 4 + len(nodes) * 16
    cursor += 4 + len(edges) * 68
    cursor += 4 + (len(nodes) + 1) * 4

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
            lon = struct.unpack_from("<f", payload, cursor)[0]
            lat = struct.unpack_from("<f", payload, cursor + 4)[0]
            cursor += 8
            assert lon == pytest.approx(expected_lon, rel=1e-5)
            assert lat == pytest.approx(expected_lat, rel=1e-5)

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
    # Header (16) + 4×(empty section counts, last is CSR offsets with one entry)
    # Specifically: 16 header + 4 (strings count=0) + 4 (nodes count=0)
    #              + 4 (edges count=0) + 4 (csr count=1) + 4 (csr entry=0)
    #              + 4 (geoms count=0)
    assert len(payload) == 16 + 4 + 4 + 4 + 4 + 4 + 4
    # CSR for zero nodes is still one entry (count = node_count + 1 = 1).
    csr_count_offset = 16 + 4 + 4 + 4
    assert struct.unpack_from("<I", payload, csr_count_offset)[0] == 1


def test_f32_or_nan_handles_none():
    assert math.isnan(f32_or_nan(None))
    assert f32_or_nan(1.5) == 1.5
    assert f32_or_nan(0) == 0.0


def _string_table_size(strings) -> int:
    """Total bytes consumed by the string table for given strings list."""
    return 4 + sum(4 + len(s.encode("utf-8")) for s in strings)
