# Routing graph binary format

This document is the source of truth for the on-disk format consumed by
`routing-core::format` and produced by the Python `RoutingGraphExporter`
(chunk 5). Any change to the format requires bumping the version field
and updating both ends in lockstep.

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

No padding, no trailers, no index. Sections are read sequentially.

### 1. Header — 16 bytes

```
offset  size  field             value / notes
------  ----  ----------------  --------------------------------
   0     4    magic             ASCII "LOCI" (0x4C 0x4F 0x43 0x49)
   4     2    version           u16 = 1
   6     2    flags             u16 = 0 (reserved)
   8     8    heuristic_floor   f64, cost-per-meter floor for A* heuristic
```

The reader MUST verify the magic and version and fail loudly on
mismatch. `flags` is reserved for future bit flags (e.g. "has CRS
metadata", "uses extended edge attributes") and MUST be 0 in v1.

`heuristic_floor` corresponds to `G.graph["heuristic_floor"]` in the
current Python implementation.

### 2. String table

A deduplicated table of `str` values referenced by `u32` indices
elsewhere in the file. Reference values of `u32::MAX` mean "null"
(no string), not "index into table at position 2^32 - 1".

```
offset  size              field            notes
------  ----------------  ---------------  -----------------------------
  16     4                count            u32 = N
  20     variable          entries          N `str` values back to back
```

Each entry is a `str`: `[u32 byte_length][UTF-8 bytes]`. No alignment,
no padding between entries.

Strings stored here include `name`, `highway`, `infra_type`, and
`segment_id` values for edges and geometries. Empty strings are valid
entries; null (i.e. "field not set") is represented by `u32::MAX` in
the reference, not by a zero-length entry.

### 3. Node table

```
field           size            notes
--------------  --------------  -----------------------
count           u32             N = node count
entries         N × 16 bytes    fixed-size records
```

Each node record:

```
offset (within record)  size  field      notes
------                  ----  ---------  --------------------
   0                    8     osm_id     u64
   8                    4     lon        f32 (degrees)
  12                    4     lat        f32 (degrees)
```

Node order in the file is the canonical `NodeIdx` ordering. The first
node has `NodeIdx = 0`, the second `NodeIdx = 1`, etc. Edges reference
nodes by `NodeIdx`, not by `osm_id`. The `osm_id` is stored for
response composition only.

### 4. Edge table

Edges are ordered by source `NodeIdx` ascending, then by an
exporter-internal tiebreak (any stable order is fine). This ordering
is what makes the CSR offsets section work.

```
field           size            notes
--------------  --------------  -----------------------
count           u32             M = directed edge count
entries         M × 56 bytes    fixed-size records
```

Each edge record (56 bytes, aligned to 4):

```
offset (within record)  size  field                   notes
------                  ----  ----------------------  ---------------------------
   0                    4     target_node_idx         u32
   4                    4     segment_id_str_idx      u32, never null
   8                    4     name_str_idx            u32 (null_idx allowed)
  12                    4     highway_str_idx         u32 (null_idx allowed)
  16                    4     infra_type_str_idx      u32 (null_idx allowed)
  20                    1     flags                   u8, bit 0 = forward
  21                    3     padding                 reserved, MUST be zero
  24                    4     length_m                f32
  28                    4     stress_cost             f32
  32                    4     speed_factor            f32
  36                    4     road_type_factor        f32
  40                    4     infrastructure_factor   f32
  44                    4     tunnel_factor           f32
  48                    4     surface_factor          f32
  52                    4     lighting_factor         f32
  56                    4     physical_cost           f32
  60                    4     intersection_cost       f32
  64                    4     crash_cost              f32
```

Wait — that's 68 bytes, not 56. Updating: each edge record is **68 bytes**.

`flags` bit 0 is the `forward` orientation flag (1 = forward, 0 =
backward), used by the response composer to know whether to reverse
segment geometry. Bits 1–7 are reserved and MUST be zero in v1.

The source node for an edge is implicit in its position; see CSR
offsets below.

Nullable f32 fields (`speed_factor`, etc.) where the source value was
NULL in Postgres are serialized as IEEE 754 NaN (`f32::NAN`). The
reader exposes these as `Option<f32>` by checking `is_nan()`.

### 5. CSR offsets

```
field           size            notes
--------------  --------------  -----------------------
count           u32             K = node_count + 1
entries         K × 4 bytes     u32 offsets into the edge table
```

`offsets[i]` is the index of the first edge with `source_node_idx = i`.
`offsets[i+1] - offsets[i]` is the out-degree of node `i`. The final
entry `offsets[node_count]` equals `edge_count`.

The reader checks: `count == node_count + 1`, offsets are monotonically
non-decreasing, and `offsets[node_count] == edge_count`. Any failure
indicates a malformed file.

### 6. Segment geometry table

```
field           size       notes
--------------  ---------  ----------------------------------
count           u32        G = number of segments with geometry
entries         variable   G geometry records
```

Each geometry record:

```
offset (within record)  size            field                 notes
------                  ----            -------------------   ------------------
   0                    4               segment_id_str_idx    u32
   4                    4               coord_count           u32 = C
   8                    C × 8           coords                C × [f32 lon, f32 lat]
```

Geometry is keyed by `segment_id_str_idx` matching the same string
index used on edges. Edges and geometries with the same
`segment_id_str_idx` share geometry; the edge's `flags.forward` bit
tells the response composer whether to use the coords as-is or
reversed.

Not every segment_id referenced by an edge needs a geometry entry: if
geometry is missing, the response composer falls back to a straight
line between the source and target nodes (matching current Python
behavior in `find_route`).

## End-of-file

After the last geometry record, the file ends. No trailer, no
checksum. The reader confirms there are zero bytes remaining and
fails if extra trailing data is present.

## Size estimates

For a Chicago-sized graph (~70k nodes, ~250k directed edges, ~125k
unique segment geometries with ~5 coords each on average):

| Section          | Bytes        | Notes                              |
| ---------------- | ------------ | ---------------------------------- |
| Header           | 16           |                                    |
| String table     | ~1 MB        | dominated by long unique segment_id values |
| Node table       | 1.1 MB       | 16 bytes × 70k                     |
| Edge table       | 17 MB        | 68 bytes × 250k                    |
| CSR offsets      | 280 KB       | 4 bytes × 70k                      |
| Segment geometry | 5 MB         | 8 bytes × 5 × 125k                 |
| **Total raw**    | **~24 MB**   |                                    |
| Gzipped          | ~7–8 MB      | expected, will measure in chunk 5  |

## Version history

* v1 — initial release.
