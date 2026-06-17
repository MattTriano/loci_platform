# Routing graph binary format

This document is the source of truth for the on-disk format consumed by
`routing-core::format` and produced by the Python `RoutingGraphExporter`.
Any change to the format requires bumping the version field and updating
both ends in lockstep: the Rust reader (`services/routing/routing-core/src/format/`)
and the Python writer (`platform/airflow/dags/loci/exports/graph_format.py`).

The current format version is **3**.

## File-level conventions

* All multi-byte integers are little-endian.
* All floats are IEEE 754 little-endian: `f32` is 4 bytes, `f64` is 8 bytes.
* Strings are UTF-8, length-prefixed by a `u32` byte length (not character count).
* The whole file is gzip-compressed on disk (`.bin.gz`). The format spec
  below describes the uncompressed payload.
* Offsets in this document are payload offsets, not file offsets.

## Type primitives

* `u8`, `u16`, `u32`, `u64` — unsigned little-endian integers.
* `f32`, `f64` — IEEE 754 little-endian floats.
* `str` — `[u32 byte_length][UTF-8 bytes]`.
* `null_idx` — sentinel value `u32::MAX` (`0xFFFFFFFF`) used in string
  references to indicate a missing value (e.g. an unnamed road).

## Top-level layout

The payload is a strict sequence of sections in this order:

1. **Header**
2. **String table**
3. **Node table**
4. **Edge table**
5. **CSR offsets**
6. **Segment geometry table**

No padding between sections, no trailers, no index. Sections are read
sequentially. After the last section the reader confirms there are zero
bytes remaining and fails if extra trailing data is present.

### 1. Header — 16 bytes

```
offset  size  field             value / notes
------  ----  ----------------  --------------------------------
   0     4    magic             ASCII "LOCI" (0x4C 0x4F 0x43 0x49)
   4     2    version           u16 = 3
   6     2    flags             u16 = 0 (reserved)
   8     8    heuristic_floor   f64, cost-per-meter floor for A* heuristic
```

The reader MUST verify the magic and version and fail loudly on
mismatch. A version other than 3 is rejected (the reader understands
exactly one version). `flags` is reserved for future bit flags and MUST
be 0; a non-zero value is rejected.

`heuristic_floor` corresponds to `G.graph["heuristic_floor"]` in the
Python exporter. It is the minimum per-meter edge cost (times a safety
margin), used to scale the great-circle distance into an admissible A*
heuristic. It is derived from the per-meter components only
(`physical_cost + crash_cost`); the per-endpoint intersection cost and
the elevation cost are deliberately excluded so the heuristic stays a
true lower bound.

### 2. String table

A deduplicated table of `str` values referenced by `u32` indices
elsewhere in the file. Reference values of `u32::MAX` mean "null"
(no string), not "index into table at position 2^32 - 1".

```
offset  size      field    notes
------  --------  -------  -----------------------------
  16     4        count    u32 = N
  20     variable  entries  N `str` values back to back
```

Each entry is a `str`: `[u32 byte_length][UTF-8 bytes]`. No alignment,
no padding between entries. Invalid UTF-8 is rejected.

Strings stored here include `name`, `highway`, `infra_type`, and
`segment_id` values for edges and geometries. Empty strings are valid
entries; null (i.e. "field not set") is represented by `u32::MAX` in
the reference, not by a zero-length entry.

### 3. Node table

```
field    size            notes
-------  --------------  -----------------------
count    u32             N = node count
entries  N × 32 bytes    fixed-size records
```

Each node record (32 bytes):

```
offset (within record)  size  field             notes
------                  ----  ----------------  -----------------------------
   0                    8     osm_id            u64
   8                    8     lon               f64 (degrees)
  16                    8     lat               f64 (degrees)
  24                    1     is_intersection   u8, 0 or 1
  25                    7     padding           reserved, MUST be zero
```

Node order in the file is the canonical `NodeIdx` ordering. The first
node has `NodeIdx = 0`, the second `NodeIdx = 1`, etc. Edges reference
nodes by `NodeIdx`, not by `osm_id`. The `osm_id` is stored for response
composition only.

The record is padded to 32 bytes so each node's f64 fields land at
8-byte-aligned offsets. The reader rejects an `is_intersection` byte
other than 0 or 1, and rejects non-zero padding bytes.

`is_intersection` is the authoritative flag for the turn-penalty logic.
It is set by the data pipeline (a node is a logical intersection iff it
appears in `<city>_intersection_costs`) and read directly at routing
time, rather than being re-derived from edge degree. The router uses it
to decide both whether a left-turn penalty can apply at the node and
whether to retain turn context in the A* search state.

### 4. Edge table

Edges are ordered by source `NodeIdx` ascending, then by a stable
exporter-internal tiebreak (parallel edges from the same source keep
their insertion order). This ordering is what makes the CSR offsets
section work.

```
field    size            notes
-------  --------------  -----------------------
count    u32             M = directed edge count
entries  M × 48 bytes    fixed-size records
```

Each edge record (48 bytes, aligned to 4):

```
offset (within record)  size  field                notes
------                  ----  -------------------  ---------------------------
   0                    4     target_node_idx      u32
   4                    4     segment_id_str_idx   u32, never null
   8                    4     name_str_idx         u32 (null_idx allowed)
  12                    4     highway_str_idx      u32 (null_idx allowed)
  16                    4     infra_type_str_idx   u32 (null_idx allowed)
  20                    1     flags                u8, bit 0 = forward
  21                    3     padding              reserved, MUST be zero
  24                    4     length_m             f32
  28                    4     stress_cost          f32
  32                    4     physical_cost        f32
  36                    4     intersection_cost    f32
  40                    4     crash_cost           f32
  44                    4     elevation_cost       f32
```

`flags` bit 0 is the `forward` orientation flag (1 = forward, 0 =
backward), used by the response composer to know whether to reverse the
shared segment geometry. Bits 1–7 are reserved and MUST be zero; the
reader rejects any reserved bit being set, and rejects non-zero padding
bytes.

The source node for an edge is implicit in its position; see CSR offsets
below.

#### Cost fields

`stress_cost` is the per-direction routing weight A* minimizes. The four
component fields are its parts, carried for inspection and the
route-segment popup:

* `physical_cost` — per-segment intrinsic stress (length × base stress
  plus surface/enclosed/lighting penalties). Direction-independent.
* `intersection_cost` — the per-endpoint intersection cost that applies
  for *this edge's traversal direction*: a forward edge carries the cost
  at its end node, a backward edge the cost at its start node.
* `crash_cost` — per-segment crash-history cost. Direction-independent.
  Zero for cities without a crash data source.
* `elevation_cost` — the directional elevation cost for this edge's
  traversal direction (uphill or downhill as appropriate). Always
  present in v3; zero on every edge for cities built without elevation
  data, and zero for genuinely flat segments.

For a graph produced by the current exporter, `stress_cost` equals
`physical_cost + intersection_cost + crash_cost + elevation_cost` for
that edge. The left-turn penalty is applied at routing time (it depends
on the incoming direction at a junction) and is therefore NOT baked into
`stress_cost` or stored here; it is reported separately in the route
response.

A cost `f32` whose source value was NULL is serialized as IEEE 754 NaN.
In practice the current exporter emits finite values for all six `f32`
fields: `length_m` and `physical_cost` are guaranteed non-null by the
exporter's query filter, and `intersection_cost`, `crash_cost`, and
`elevation_cost` are coalesced to 0 before serialization. The NaN
convention is retained as the format-level encoding for "missing"; the
reader reads these fields as raw `f32` and does not special-case NaN, so
any consumer that could encounter a NaN is responsible for treating it
as missing.

### 5. CSR offsets

```
field    size            notes
-------  --------------  -----------------------
count    u32             K = node_count + 1
entries  K × 4 bytes     u32 offsets into the edge table
```

`offsets[i]` is the index of the first edge with `source_node_idx = i`.
`offsets[i+1] - offsets[i]` is the out-degree of node `i`. The final
entry `offsets[node_count]` equals `edge_count`.

The reader checks: `count == node_count + 1`, offsets are monotonically
non-decreasing, and `offsets[node_count] == edge_count`. Any failure
indicates a malformed file.

### 6. Segment geometry table

```
field    size       notes
-------  ---------  ----------------------------------
count    u32        G = number of segments with geometry
entries  variable   G geometry records
```

Each geometry record:

```
offset (within record)  size            field                notes
------                  ----            -------------------  -----------------------
   0                    4               segment_id_str_idx   u32, never null
   4                    4               coord_count          u32 = C
   8                    C × 16          coords               C × [f64 lon, f64 lat]
```

Geometry is keyed by `segment_id_str_idx`, matching the same string
index used on edges. Each coordinate pair is two f64 values (lon, lat),
16 bytes per coordinate.

Edges and geometries with the same `segment_id_str_idx` share geometry;
the edge's `flags` forward bit tells the response composer whether to
use the coords as-is or reversed. Not every segment_id referenced by an
edge needs a geometry entry: if geometry is missing, the response
composer falls back to a straight line between the source and target
nodes.

## End-of-file

After the last geometry record, the file ends. No trailer, no checksum.
The reader confirms there are zero bytes remaining and fails if extra
trailing data is present.

## Size estimates

Rough order-of-magnitude figures for a Chicago-sized graph (~70k nodes,
~250k directed edges, ~125k unique segment geometries with ~5 coords
each on average). These are estimates from the record sizes, not
measured values.

| Section          | Bytes        | Notes                                       |
| ---------------- | ------------ | ------------------------------------------- |
| Header           | 16           |                                             |
| String table     | ~1 MB        | dominated by long unique segment_id values  |
| Node table       | ~2.2 MB      | 32 bytes × 70k                              |
| Edge table       | ~12 MB       | 48 bytes × 250k                            |
| CSR offsets      | ~280 KB      | 4 bytes × (70k + 1)                         |
| Segment geometry | ~11 MB       | (8 + 16 × 5) bytes × 125k                   |
| **Total raw**    | **~27 MB**   |                                             |
| Gzipped          | ~8–9 MB      | dominated by string table and coordinates   |

## Version history

* **v1** — initial release. Node coordinates were f32 (16-byte node
  record). The edge record carried six multiplicative factor fields
  (speed / road_type / infrastructure / tunnel / surface / lighting)
  alongside the costs, for a 68-byte record. Segment geometry coords
  were f32.
* **v2** — node coordinates promoted to f64, and the node record gained
  an `is_intersection` u8 flag plus padding to 32 bytes (was 16). The
  edge record dropped the six legacy multiplicative factor fields in
  favor of the additive cost model, keeping `physical_cost`,
  `intersection_cost`, and `crash_cost` — shrinking from 68 to 44 bytes.
  Segment geometry coordinates promoted from f32 to f64 (8 to 16 bytes
  per coordinate). Header, string table, and CSR offsets unchanged.
* **v3** — the edge record gained a fourth cost component,
  `elevation_cost`, appended after `crash_cost` — growing from 44 to 48
  bytes. No other section changed.
