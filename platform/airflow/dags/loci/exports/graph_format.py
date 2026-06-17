# /loci_platform/platform/airflow/dags/loci/exports/graph_format.py
"""
Binary writer for the routing graph format.

This module is intentionally decoupled from the database and NetworkX:
it takes plain Python data (nodes, edges, geometries) and emits bytes
matching the format specified in services/routing/docs/graph-format.md.

The contract document is the spec; this module implements it. Any
change here requires a corresponding change in the Rust reader and a
bump of FORMAT_VERSION.

Format v2 changes from v1:
  - Node coordinates promoted from f32 to f64.
  - Node record gains an is_intersection u8 flag and padding to 32 bytes.
  - Segment geometry coordinates promoted from f32 to f64.
  - Edge record drops the six legacy multiplicative factor fields
    (speed/road_type/infrastructure/tunnel/surface/lighting), shrinking
    from 68 to 44 bytes. The additive cost model that replaced them
    keeps only physical_cost, intersection_cost, and crash_cost.

Format v3 changes from v2:
  - The edge record is now 48 bytes; gained elevation_cost
"""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from typing import IO

MAGIC = b"LOCI"
FORMAT_VERSION = 3
NULL_STR_IDX = 0xFFFFFFFF
EDGE_FLAG_FORWARD = 0b0000_0001

# Pre-compiled struct formatters. The leading "<" enforces little-endian.
_U16_LE = struct.Struct("<H")
_U32_LE = struct.Struct("<I")
_U64_LE = struct.Struct("<Q")
_F32_LE = struct.Struct("<f")
_F64_LE = struct.Struct("<d")

# Node record is padded to 32 bytes so each one's f64 fields land at
# 8-byte-aligned offsets. Layout: 8 (osm_id) + 8 (lon) + 8 (lat)
# + 1 (is_intersection) + 7 (padding) = 32.
_NODE_PADDING = b"\x00" * 7


@dataclass
class WriteNode:
    """A node in the order it will be assigned NodeIdx 0, 1, 2, ..."""

    osm_id: int
    lon: float
    lat: float
    is_intersection: bool


@dataclass
class WriteEdge:
    """A directed edge in CSR order.

    String references (segment_id_str_idx and the three optional fields)
    are indices into a string table that the caller deduplicates. The
    segment_id index is never NULL_STR_IDX; the other three may be.

    `source_node_idx` is included for ordering / CSR construction but
    not written to disk; the writer derives implicit source nodes from
    the position in the edge table.

    `stress_cost` is the per-direction routing weight A* minimizes;
    physical_cost / intersection_cost / crash_cost are its components,
    carried for inspection and the route-segment popup.
    """

    source_node_idx: int
    target_node_idx: int
    segment_id_str_idx: int
    name_str_idx: int  # NULL_STR_IDX if missing
    highway_str_idx: int  # NULL_STR_IDX if missing
    infra_type_str_idx: int  # NULL_STR_IDX if missing
    forward: bool
    length_m: float
    stress_cost: float
    physical_cost: float
    intersection_cost: float
    crash_cost: float
    elevation_cost: float


@dataclass
class WriteSegmentGeometry:
    segment_id_str_idx: int
    coords: list[tuple[float, float]]  # (lon, lat) pairs, written as f64


def write_graph(
    out: IO[bytes],
    *,
    heuristic_floor: float,
    strings: list[str],
    nodes: list[WriteNode],
    edges: list[WriteEdge],
    segment_geometries: list[WriteSegmentGeometry],
) -> None:
    """Serialize a routing graph to `out` in the binary format.

    The caller is responsible for:
      - Deduplicating strings.
      - Sorting edges by source_node_idx ascending (CSR ordering).
      - Ensuring all string indices reference valid entries in `strings`
        (or NULL_STR_IDX for nullable fields).

    The writer computes the CSR offset table from the edge ordering.

    `out` should be an opened file-like object in binary mode. The
    caller is responsible for gzip wrapping if desired (typically yes;
    the final .bin.gz on S3 is gzipped).
    """
    # --- Header ---
    out.write(MAGIC)
    out.write(_U16_LE.pack(FORMAT_VERSION))
    out.write(_U16_LE.pack(0))  # flags reserved
    out.write(_F64_LE.pack(heuristic_floor))

    # --- String table ---
    out.write(_U32_LE.pack(len(strings)))
    for s in strings:
        encoded = s.encode("utf-8")
        out.write(_U32_LE.pack(len(encoded)))
        out.write(encoded)

    # --- Node table ---
    out.write(_U32_LE.pack(len(nodes)))
    for n in nodes:
        out.write(_U64_LE.pack(n.osm_id))
        out.write(_F64_LE.pack(n.lon))
        out.write(_F64_LE.pack(n.lat))
        out.write(bytes([1 if n.is_intersection else 0]))
        out.write(_NODE_PADDING)

    # --- Edge table + CSR offsets ---
    # CSR offsets are computed during the edge write pass: we record
    # the edge index at each source-node transition. The edges must
    # already be sorted by source_node_idx ascending.
    node_count = len(nodes)
    csr_offsets = [0] * (node_count + 1)

    _validate_edge_ordering(edges)

    out.write(_U32_LE.pack(len(edges)))
    current_source = 0
    for edge_idx, e in enumerate(edges):
        # Advance csr_offsets[i] for every source node we've passed.
        while current_source < e.source_node_idx:
            current_source += 1
            csr_offsets[current_source] = edge_idx
        _write_edge(out, e)
    # Final offset(s): any source nodes after the last edge get the
    # total edge count.
    while current_source < node_count:
        current_source += 1
        csr_offsets[current_source] = len(edges)

    # --- CSR offsets table ---
    out.write(_U32_LE.pack(len(csr_offsets)))
    for offset in csr_offsets:
        out.write(_U32_LE.pack(offset))

    # --- Segment geometry table ---
    out.write(_U32_LE.pack(len(segment_geometries)))
    for g in segment_geometries:
        out.write(_U32_LE.pack(g.segment_id_str_idx))
        out.write(_U32_LE.pack(len(g.coords)))
        for lon, lat in g.coords:
            out.write(_F64_LE.pack(lon))
            out.write(_F64_LE.pack(lat))


def _write_edge(out: IO[bytes], e: WriteEdge) -> None:
    out.write(_U32_LE.pack(e.target_node_idx))
    out.write(_U32_LE.pack(e.segment_id_str_idx))
    out.write(_U32_LE.pack(e.name_str_idx))
    out.write(_U32_LE.pack(e.highway_str_idx))
    out.write(_U32_LE.pack(e.infra_type_str_idx))
    out.write(bytes([EDGE_FLAG_FORWARD if e.forward else 0]))
    out.write(b"\x00\x00\x00")  # 3-byte padding
    out.write(_F32_LE.pack(e.length_m))
    out.write(_F32_LE.pack(e.stress_cost))
    out.write(_F32_LE.pack(e.physical_cost))
    out.write(_F32_LE.pack(e.intersection_cost))
    out.write(_F32_LE.pack(e.crash_cost))
    out.write(_F32_LE.pack(e.elevation_cost))


def _validate_edge_ordering(edges: list[WriteEdge]) -> None:
    """Ensure edges are sorted by source_node_idx ascending.

    A violation indicates a bug in the caller (the routing graph
    exporter). Fail loudly with the offending pair so the exporter can
    be debugged.
    """
    for i in range(1, len(edges)):
        if edges[i].source_node_idx < edges[i - 1].source_node_idx:
            raise ValueError(
                f"edges not sorted by source_node_idx at position {i}: "
                f"{edges[i - 1].source_node_idx} -> {edges[i].source_node_idx}"
            )


def f32_or_nan(value: float | None) -> float:
    """Coerce an optional float to f32-representable, mapping None to NaN.

    Helper for callers building WriteEdge from SQL rows where some
    columns may be NULL. The format encodes "missing" as IEEE 754 NaN.
    """
    if value is None:
        return math.nan
    return float(value)
