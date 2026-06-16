# /loci_platform/platform/tests/exports/test_graph_export.py
"""
Tests for the routing graph exporter (graph_export.py).

graph_format.py (the byte-level writer) is covered by test_graph_format.py.
This file covers the layer above it: building a MultiDiGraph from SQL-shaped
rows and turning it into WriteNode/WriteEdge/WriteSegmentGeometry records.
This is the layer where the recent bugs lived (parallel-segment dropping,
intersection-cost direction, per-node is_intersection, cost composition).

Each unit test builds a small nx.MultiDiGraph directly — no database, no
SQL. The single end-to-end test drives RoutingGraphExporter.export() with a
fake PostgresEngine, then gunzips and decodes the bytes to check the full
path. The decode logic mirrors the layout in services/routing/docs/graph-format.md.
"""

import gzip
import json
import struct
from decimal import Decimal

import networkx as nx
import pytest
from loci.exports.graph_export import (
    RoutingGraphExporter,
    _coerce_cost,
    _f,
    build_edges_and_strings,
    build_node_table,
    build_segment_geometries,
)
from loci.exports.graph_format import EDGE_FLAG_FORWARD, NULL_STR_IDX

# ----------------------------------------------------------------------
# Fixtures / builders
# ----------------------------------------------------------------------


def _add_node(G, osm_id, *, x=0.0, y=0.0, is_intersection=False):
    G.add_node(osm_id, x=x, y=y, is_intersection=is_intersection)


def _edge_attrs(
    segment_id,
    *,
    length_m=100.0,
    name=None,
    highway=None,
    infra_type=None,
    physical_cost=100.0,
    crash_cost=0.0,
    intersection_cost_at_start=0.0,
    intersection_cost_at_end=0.0,
):
    """Edge attribute dict matching what _build_graph attaches per edge.

    `forward` is set separately on add_edge, mirroring the real exporter.
    """
    return {
        "segment_id": segment_id,
        "length_m": length_m,
        "name": name,
        "highway": highway,
        "infra_type": infra_type,
        "physical_cost": physical_cost,
        "crash_cost": crash_cost,
        "intersection_cost_at_start": intersection_cost_at_start,
        "intersection_cost_at_end": intersection_cost_at_end,
    }


# ----------------------------------------------------------------------
# build_node_table
# ----------------------------------------------------------------------


def test_node_table_assigns_nodeidx_in_iteration_order():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    _add_node(G, 30)

    nodes, osm_to_idx = build_node_table(G)

    assert [n.osm_id for n in nodes] == [10, 20, 30]
    assert osm_to_idx == {10: 0, 20: 1, 30: 2}


def test_node_table_osm_to_idx_is_consistent_with_node_list():
    G = nx.MultiDiGraph()
    for osm_id in (101, 202, 303, 404):
        _add_node(G, osm_id)

    nodes, osm_to_idx = build_node_table(G)

    for osm_id, idx in osm_to_idx.items():
        assert nodes[idx].osm_id == osm_id


def test_node_table_carries_is_intersection():
    G = nx.MultiDiGraph()
    _add_node(G, 10, is_intersection=True)
    _add_node(G, 20, is_intersection=False)
    # A node missing the attribute entirely should default to False.
    G.add_node(30, x=0.0, y=0.0)

    nodes, _ = build_node_table(G)
    by_id = {n.osm_id: n for n in nodes}

    assert by_id[10].is_intersection is True
    assert by_id[20].is_intersection is False
    assert by_id[30].is_intersection is False


def test_node_table_coerces_coords_to_float():
    G = nx.MultiDiGraph()
    # Integers and Decimals (as psycopg might hand back) must become floats.
    G.add_node(10, x=1, y=Decimal("2.5"), is_intersection=False)

    nodes, _ = build_node_table(G)

    assert isinstance(nodes[0].lon, float) and nodes[0].lon == 1.0
    assert isinstance(nodes[0].lat, float) and nodes[0].lat == 2.5


def test_node_table_empty_graph():
    nodes, osm_to_idx = build_node_table(nx.MultiDiGraph())
    assert nodes == []
    assert osm_to_idx == {}


def test_node_table_single_node():
    G = nx.MultiDiGraph()
    _add_node(G, 42, x=-87.6, y=41.9, is_intersection=True)

    nodes, osm_to_idx = build_node_table(G)

    assert len(nodes) == 1
    assert osm_to_idx == {42: 0}
    assert nodes[0].osm_id == 42
    assert nodes[0].is_intersection is True


# ----------------------------------------------------------------------
# build_edges_and_strings
# ----------------------------------------------------------------------


def test_forward_segment_makes_one_directed_edge():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    G.add_edge(10, 20, forward=True, **_edge_attrs("S0"))

    _, osm_to_idx = build_node_table(G)
    edges, _ = build_edges_and_strings(G, osm_to_idx)

    assert len(edges) == 1
    assert edges[0].forward is True
    assert edges[0].source_node_idx == osm_to_idx[10]
    assert edges[0].target_node_idx == osm_to_idx[20]


def test_bidirectional_makes_two_edges_with_opposite_flags():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    # Mirrors _build_graph: bidirectional -> both orientations.
    G.add_edge(10, 20, forward=True, **_edge_attrs("S0"))
    G.add_edge(20, 10, forward=False, **_edge_attrs("S0"))

    _, osm_to_idx = build_node_table(G)
    edges, _ = build_edges_and_strings(G, osm_to_idx)

    assert len(edges) == 2
    by_dir = {e.forward: e for e in edges}
    assert set(by_dir) == {True, False}
    assert by_dir[True].source_node_idx == osm_to_idx[10]
    assert by_dir[True].target_node_idx == osm_to_idx[20]
    assert by_dir[False].source_node_idx == osm_to_idx[20]
    assert by_dir[False].target_node_idx == osm_to_idx[10]


def test_forward_stress_uses_intersection_cost_at_end():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    G.add_edge(
        10,
        20,
        forward=True,
        **_edge_attrs(
            "S0",
            physical_cost=100.0,
            crash_cost=5.0,
            intersection_cost_at_start=10.0,
            intersection_cost_at_end=50.0,
        ),
    )

    _, osm_to_idx = build_node_table(G)
    edges, _ = build_edges_and_strings(G, osm_to_idx)

    # Forward edge approaches the segment's end node.
    assert edges[0].intersection_cost == pytest.approx(50.0)
    assert edges[0].stress_cost == pytest.approx(100.0 + 5.0 + 50.0)


def test_backward_stress_uses_intersection_cost_at_start():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    G.add_edge(
        20,
        10,
        forward=False,
        **_edge_attrs(
            "S0",
            physical_cost=100.0,
            crash_cost=5.0,
            intersection_cost_at_start=10.0,
            intersection_cost_at_end=50.0,
        ),
    )

    _, osm_to_idx = build_node_table(G)
    edges, _ = build_edges_and_strings(G, osm_to_idx)

    # Backward edge approaches the segment's start node.
    assert edges[0].intersection_cost == pytest.approx(10.0)
    assert edges[0].stress_cost == pytest.approx(100.0 + 5.0 + 10.0)


def test_string_interning_dedups_and_preserves_order():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    _add_node(G, 30)
    G.add_edge(10, 20, forward=True, **_edge_attrs("S0", highway="residential"))
    G.add_edge(20, 30, forward=True, **_edge_attrs("S1", highway="residential"))

    _, osm_to_idx = build_node_table(G)
    edges, strings = build_edges_and_strings(G, osm_to_idx)

    # "residential" interned once, shared by both edges.
    assert list(strings).count("residential") == 1
    hwy_idx = strings["residential"]
    assert all(e.highway_str_idx == hwy_idx for e in edges)
    # First-insertion order: S0 (seg) at 0, residential next, S1 after.
    assert strings["S0"] == 0
    assert strings["residential"] == 1
    assert strings["S1"] == 2


def test_none_nullable_field_returns_null_str_idx():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    # name omitted -> None -> NULL_STR_IDX (nullable). highway present.
    G.add_edge(10, 20, forward=True, **_edge_attrs("S0", name=None, highway="primary"))

    _, osm_to_idx = build_node_table(G)
    edges, _ = build_edges_and_strings(G, osm_to_idx)

    assert edges[0].name_str_idx == NULL_STR_IDX
    assert edges[0].highway_str_idx != NULL_STR_IDX


def test_none_segment_id_raises():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    G.add_edge(10, 20, forward=True, **_edge_attrs(None))  # segment_id None

    _, osm_to_idx = build_node_table(G)
    with pytest.raises(ValueError):
        build_edges_and_strings(G, osm_to_idx)


def test_edges_sorted_by_source_node_idx():
    G = nx.MultiDiGraph()
    # Add nodes so idx order is 10->0, 20->1, 30->2.
    _add_node(G, 10)
    _add_node(G, 20)
    _add_node(G, 30)
    # Add edges with sources out of order (30, then 10, then 20).
    G.add_edge(30, 10, forward=True, **_edge_attrs("S2"))
    G.add_edge(10, 20, forward=True, **_edge_attrs("S0"))
    G.add_edge(20, 30, forward=True, **_edge_attrs("S1"))

    _, osm_to_idx = build_node_table(G)
    edges, _ = build_edges_and_strings(G, osm_to_idx)

    sources = [e.source_node_idx for e in edges]
    assert sources == sorted(sources)


def test_parallel_segments_are_all_preserved():
    G = nx.MultiDiGraph()
    _add_node(G, 10)
    _add_node(G, 20)
    # Two distinct ways between the same pair of nodes — the bug the
    # MultiDiGraph switch fixed. Both must survive.
    G.add_edge(10, 20, forward=True, **_edge_attrs("S0", highway="primary"))
    G.add_edge(10, 20, forward=True, **_edge_attrs("S1", highway="cycleway"))

    _, osm_to_idx = build_node_table(G)
    edges, strings = build_edges_and_strings(G, osm_to_idx)

    assert len(edges) == 2
    seg_indices = {e.segment_id_str_idx for e in edges}
    assert {strings["S0"], strings["S1"]} == seg_indices


# ----------------------------------------------------------------------
# build_segment_geometries
# ----------------------------------------------------------------------


def test_referenced_geometry_is_emitted():
    strings = {"S0": 0}
    seg_geom = {"S0": ((-87.6, 41.8), (-87.61, 41.81))}

    records = build_segment_geometries(seg_geom, strings)

    assert len(records) == 1
    assert records[0].segment_id_str_idx == 0
    assert records[0].coords == [(-87.6, 41.8), (-87.61, 41.81)]


def test_orphaned_geometry_is_skipped():
    # S1 has geometry but no string-table entry (orphaned by component
    # filtering before this point) -> dropped silently.
    strings = {"S0": 0}
    seg_geom = {
        "S0": ((0.0, 0.0), (1.0, 1.0)),
        "S1": ((2.0, 2.0), (3.0, 3.0)),
    }

    records = build_segment_geometries(seg_geom, strings)

    assert [r.segment_id_str_idx for r in records] == [0]


def test_geometry_coords_coerced_to_float():
    strings = {"S0": 0}
    # Decimal coords (psycopg numerics) must come out as plain floats.
    seg_geom = {"S0": ((Decimal("-87.6"), Decimal("41.8")), (1, 2))}

    records = build_segment_geometries(seg_geom, strings)

    coords = records[0].coords
    assert all(isinstance(v, float) for pair in coords for v in pair)
    assert coords == [(-87.6, 41.8), (1.0, 2.0)]


# ----------------------------------------------------------------------
# _filter_small_components
# ----------------------------------------------------------------------


def _exporter(min_component_size):
    """An exporter with no real engine — only used for methods that
    don't touch the DB (_filter_small_components, _parse_geojson_coords)."""
    return RoutingGraphExporter(
        engine=None,
        city="testcity",
        marts_schema="marts",
        min_component_size=min_component_size,
    )


def _chain(G, node_ids, seg_prefix):
    """Add a connected path through node_ids with forward edges."""
    for n in node_ids:
        if n not in G:
            _add_node(G, n)
    for i in range(len(node_ids) - 1):
        u, v = node_ids[i], node_ids[i + 1]
        G.add_edge(u, v, forward=True, **_edge_attrs(f"{seg_prefix}_{i}"))


def test_small_component_is_removed():
    G = nx.MultiDiGraph()
    G.graph["segment_geometry"] = {}
    _chain(G, [1, 2, 3, 4, 5], "big")  # 5 nodes
    _chain(G, [101, 102], "small")  # 2 nodes

    out = _exporter(min_component_size=4)._filter_small_components(G)

    remaining = set(out.nodes)
    assert remaining == {1, 2, 3, 4, 5}
    assert 101 not in remaining and 102 not in remaining


def test_large_component_is_retained():
    G = nx.MultiDiGraph()
    G.graph["segment_geometry"] = {}
    _chain(G, [1, 2, 3, 4, 5], "big")

    out = _exporter(min_component_size=4)._filter_small_components(G)

    assert set(out.nodes) == {1, 2, 3, 4, 5}


def test_orphaned_geometry_dropped_after_filtering():
    G = nx.MultiDiGraph()
    _chain(G, [1, 2, 3, 4, 5], "big")
    _chain(G, [101, 102], "small")
    # Geometry for one big-component segment and one small-component segment.
    G.graph["segment_geometry"] = {
        "big_0": ((0.0, 0.0), (1.0, 1.0)),
        "small_0": ((2.0, 2.0), (3.0, 3.0)),
    }

    out = _exporter(min_component_size=4)._filter_small_components(G)

    geom = out.graph["segment_geometry"]
    assert "big_0" in geom
    assert "small_0" not in geom


def test_min_component_size_one_returns_graph_unchanged():
    G = nx.MultiDiGraph()
    G.graph["segment_geometry"] = {}
    _chain(G, [1, 2], "a")
    _chain(G, [101, 102], "b")  # disconnected, would normally be small

    out = _exporter(min_component_size=1)._filter_small_components(G)

    # Same object, nothing removed.
    assert out is G
    assert set(out.nodes) == {1, 2, 101, 102}


def test_multiple_large_components_logs_warning(caplog):
    G = nx.MultiDiGraph()
    G.graph["segment_geometry"] = {}
    _chain(G, [1, 2, 3, 4, 5], "a")  # large
    _chain(G, [11, 12, 13, 14, 15], "b")  # also large

    with caplog.at_level("WARNING"):
        _exporter(min_component_size=3)._filter_small_components(G)

    assert any(
        "large components" in r.message or "large component" in r.message for r in caplog.records
    )


# ----------------------------------------------------------------------
# _parse_geojson_coords
# ----------------------------------------------------------------------


def test_parse_geojson_valid_linestring():
    geojson = json.dumps({"type": "LineString", "coordinates": [[-87.6, 41.8], [-87.61, 41.81]]})
    coords = RoutingGraphExporter._parse_geojson_coords(geojson)
    assert coords == ((-87.6, 41.8), (-87.61, 41.81))


def test_parse_geojson_none_or_empty_returns_none():
    assert RoutingGraphExporter._parse_geojson_coords(None) is None
    assert RoutingGraphExporter._parse_geojson_coords("") is None


def test_parse_geojson_malformed_returns_none():
    assert RoutingGraphExporter._parse_geojson_coords("{not json") is None


def test_parse_geojson_missing_coordinates_returns_none():
    geojson = json.dumps({"type": "LineString"})  # no coordinates key
    assert RoutingGraphExporter._parse_geojson_coords(geojson) is None


# ----------------------------------------------------------------------
# Small coercion helpers
# ----------------------------------------------------------------------


def test_coerce_cost_maps_none_and_nan_to_zero():
    assert _coerce_cost(None) == 0.0
    assert _coerce_cost(float("nan")) == 0.0
    assert _coerce_cost(3) == 3.0
    assert _coerce_cost("2.5") == 2.5


def test_f_passes_none_through_and_coerces_numbers():
    assert _f(None) is None
    assert _f(3) == 3.0
    assert _f(Decimal("1.5")) == 1.5


# ----------------------------------------------------------------------
# End-to-end: fake engine -> export() -> gunzip -> decode
# ----------------------------------------------------------------------


class _FloorCol:
    def __init__(self, value):
        self.iloc = [value]


class _FloorFrame:
    """Minimal stand-in for the DataFrame returned by engine.query()."""

    def __init__(self, value, empty=False):
        self._value = value
        self.empty = empty

    def __getitem__(self, key):
        assert key == "floor"
        return _FloorCol(self._value)


class _FakeEngine:
    """Stubs the two PostgresEngine methods the exporter calls."""

    def __init__(self, rows, floor):
        self._rows = rows
        self._floor = floor

    def query_batches(self, query, batch_size):
        yield self._rows

    def query(self, sql):
        return _FloorFrame(self._floor)


def _row(
    segment_id,
    start_node_id,
    end_node_id,
    direction,
    *,
    start_lon,
    start_lat,
    end_lon,
    end_lat,
    start_is_intersection,
    end_is_intersection,
    physical_cost,
    intersection_cost_at_start=0.0,
    intersection_cost_at_end=0.0,
    elevation_cost_forward=0.0,
    elevation_cost_backward=0.0,
    crash_cost=0.0,
    length_m=100.0,
    name=None,
    highway="residential",
    infra_type=None,
):
    geom = json.dumps(
        {"type": "LineString", "coordinates": [[start_lon, start_lat], [end_lon, end_lat]]}
    )
    return {
        "segment_id": segment_id,
        "start_node_id": start_node_id,
        "end_node_id": end_node_id,
        "direction": direction,
        "name": name,
        "highway": highway,
        "infra_type": infra_type,
        "length_m": length_m,
        "physical_cost": physical_cost,
        "crash_cost": crash_cost,
        "intersection_cost_at_start": intersection_cost_at_start,
        "intersection_cost_at_end": intersection_cost_at_end,
        "elevation_cost_forward": elevation_cost_forward,
        "elevation_cost_backward": elevation_cost_backward,
        "start_is_intersection": start_is_intersection,
        "end_is_intersection": end_is_intersection,
        "highway_class": "local",
        "infra_tier": "none",
        "base_stress_per_meter": 1.8,
        "surface_penalty": 0.0,
        "enclosed_penalty": 0.0,
        "lighting_penalty": 0.0,
        "geom_geojson": geom,
        "start_lon": start_lon,
        "start_lat": start_lat,
        "end_lon": end_lon,
        "end_lat": end_lat,
    }


def _decode_graph(path):
    """Decode header, string table, node table, and edge table from a
    gzipped graph file. Mirrors services/routing/docs/graph-format.md.
    """
    with gzip.open(path, "rb") as fh:
        buf = fh.read()

    pos = 0

    def u16():
        nonlocal pos
        v = struct.unpack_from("<H", buf, pos)[0]
        pos += 2
        return v

    def u32():
        nonlocal pos
        v = struct.unpack_from("<I", buf, pos)[0]
        pos += 4
        return v

    # Header
    assert buf[0:4] == b"LOCI"
    pos = 4
    version = u16()
    _flags = u16()
    heuristic_floor = struct.unpack_from("<d", buf, pos)[0]
    pos += 8

    # String table
    str_count = u32()
    strings = []
    for _ in range(str_count):
        slen = u32()
        strings.append(buf[pos : pos + slen].decode("utf-8"))
        pos += slen

    # Node table (32 bytes each)
    node_count = u32()
    nodes = []
    for _ in range(node_count):
        osm_id = struct.unpack_from("<Q", buf, pos)[0]
        is_intersection = bool(buf[pos + 24])
        nodes.append({"osm_id": osm_id, "is_intersection": is_intersection})
        pos += 32

    # Edge table (48 bytes each)
    edge_count = u32()
    edges = []
    for _ in range(edge_count):
        rec = buf[pos : pos + 48]
        edges.append(
            {
                "target": struct.unpack_from("<I", rec, 0)[0],
                "forward": bool(rec[20] & EDGE_FLAG_FORWARD),
                "length_m": struct.unpack_from("<f", rec, 24)[0],
                "stress_cost": struct.unpack_from("<f", rec, 28)[0],
                "physical_cost": struct.unpack_from("<f", rec, 32)[0],
                "intersection_cost": struct.unpack_from("<f", rec, 36)[0],
                "crash_cost": struct.unpack_from("<f", rec, 40)[0],
                "elevation_cost": struct.unpack_from("<f", rec, 44)[0],
            }
        )
        pos += 48

    return {
        "version": version,
        "heuristic_floor": heuristic_floor,
        "strings": strings,
        "nodes": nodes,
        "edges": edges,
    }


def test_export_end_to_end(tmp_path):
    # Three nodes A(10) - B(20) - C(30). B is a real intersection.
    # S0: A<->B bidirectional, with intersection cost only at B (end).
    # S1: B->C forward only.
    rows = [
        _row(
            "S0",
            10,
            20,
            "bidirectional",
            start_lon=-87.60,
            start_lat=41.80,
            end_lon=-87.61,
            end_lat=41.81,
            start_is_intersection=False,
            end_is_intersection=True,
            physical_cost=100.0,
            intersection_cost_at_start=0.0,
            intersection_cost_at_end=50.0,
        ),
        _row(
            "S1",
            20,
            30,
            "forward",
            start_lon=-87.61,
            start_lat=41.81,
            end_lon=-87.62,
            end_lat=41.82,
            start_is_intersection=True,
            end_is_intersection=False,
            physical_cost=200.0,
            elevation_cost_forward=30.0,
        ),
    ]
    engine = _FakeEngine(rows, floor=0.02)

    # min_component_size=1 so the tiny test graph isn't filtered away.
    exporter = RoutingGraphExporter(
        engine=engine,
        city="testcity",
        marts_schema="marts",
        min_component_size=1,
    )
    out_path = tmp_path / "graph.bin.gz"
    exporter.export(out_path)

    decoded = _decode_graph(out_path)

    assert decoded["version"] == 3
    # heuristic_floor = raw floor * 0.95 safety margin.
    assert decoded["heuristic_floor"] == pytest.approx(0.02 * 0.95)

    # 3 nodes; exactly one (B / osm 20) flagged as intersection.
    assert len(decoded["nodes"]) == 3
    by_osm = {n["osm_id"]: n for n in decoded["nodes"]}
    assert by_osm[20]["is_intersection"] is True
    assert by_osm[10]["is_intersection"] is False
    assert by_osm[30]["is_intersection"] is False

    # 3 directed edges: S0 forward (A->B), S0 backward (B->A), S1 forward (B->C).
    assert len(decoded["edges"]) == 3

    idx = {n["osm_id"]: i for i, n in enumerate(decoded["nodes"])}
    fwd_ab = next(e for e in decoded["edges"] if e["forward"] and e["target"] == idx[20])
    bwd_ba = next(e for e in decoded["edges"] if not e["forward"] and e["target"] == idx[10])

    # Forward A->B pays B's (end) intersection cost; backward B->A pays
    # A's (start) cost, which is zero.
    assert fwd_ab["physical_cost"] == pytest.approx(100.0)
    assert fwd_ab["intersection_cost"] == pytest.approx(50.0)
    assert fwd_ab["stress_cost"] == pytest.approx(150.0)

    assert bwd_ba["intersection_cost"] == pytest.approx(0.0)
    assert bwd_ba["stress_cost"] == pytest.approx(100.0)

    fwd_bc = next(e for e in decoded["edges"] if e["forward"] and e["target"] == idx[30])
    assert fwd_bc["elevation_cost"] == pytest.approx(30.0)
    assert fwd_bc["stress_cost"] == pytest.approx(200.0 + 30.0)  # physical + elevation


def test_export_falls_back_to_zero_floor_when_query_empty(tmp_path):
    rows = [
        _row(
            "S0",
            10,
            20,
            "forward",
            start_lon=-87.60,
            start_lat=41.80,
            end_lon=-87.61,
            end_lat=41.81,
            start_is_intersection=False,
            end_is_intersection=False,
            physical_cost=100.0,
        )
    ]

    class _EmptyFloorEngine(_FakeEngine):
        def query(self, sql):
            return _FloorFrame(None, empty=True)

    exporter = RoutingGraphExporter(
        engine=_EmptyFloorEngine(rows, floor=None),
        city="testcity",
        marts_schema="marts",
        min_component_size=1,
    )
    out_path = tmp_path / "graph.bin.gz"
    exporter.export(out_path)

    decoded = _decode_graph(out_path)
    assert decoded["heuristic_floor"] == pytest.approx(0.0)
