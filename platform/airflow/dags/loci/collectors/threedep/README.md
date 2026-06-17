# 3DEP elevation collector

Collection tooling for the [USGS 3D Elevation Program (3DEP)](https://www.usgs.gov/3d-elevation-program) — the national elevation layer of The National Map. This collector pulls bare-earth digital elevation models (DEMs) into a PostGIS **raster** table (typically one per region) that downstream queries sample with `ST_Value`.

This source covers **the contiguous US** (plus partial Alaska, Hawaii, and territories). Reach for this collector when you need US elevation data as a queryable raster.

Unlike the tabular collectors (CMS, Socrata, CKAN, OSM), the payload here is raster, not rows. The mechanics that make raster fit the standard `StagedIngest` path are described under [A raster source](#a-raster-source) below.

## The publication model

3DEP's seamless products are pre-staged as **1° × 1° GeoTIFF tiles** at predictable URLs under the public TNM S3 bucket, with no API or auth required:

```
https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/
    current/n42w088/USGS_13_n42w088.tif
```

Tiles are named by their **northwest corner** (`nNNwWWW`), so `n42w088` covers latitude 41–42° N and longitude 88–87° W. They are distributed in **NAD83 (EPSG:4269)** with elevations in **NAVD88 meters** — and 4269 is the `BBox` default SRID, so tile selection and clipping need no reprojection. The `current/` folder is updated in place as new data lands; prior blocks are retained under `historical/` with a date suffix (we only read `current/`).

Two seamless products are supported, both on the same `nNNwWWW` grid:

| product | resolution | notes |
|---|---|---|
| `"13"` | 1/3 arc-second (~10 m) | **default** |
| `"1"`  | 1 arc-second (~30 m)   | ~9× smaller tiles; coarser |

The 1 m product uses a different, project-based tiling and is out of scope.

## Classes

Standard four-class collector interface, built on the shared raster tooling in `loci/raster/`:

* `ThreeDEPClient` (`client.py`) — thin HTTP client over the TNM bucket: HEAD existence checks and streamed tile download. Also holds the pure tile addressing (`tile_name`, `tiles_for_bbox`, `tile_url`).
* `ThreeDEPMetadata` (`metadata.py`) — source exploration: list products, enumerate the tiles a region needs, and pre-flight which are actually staged (`coverage`, `missing`, `describe`).
* `ThreeDEPSpec` (`spec.py`) — declares the region (a `BBox`), product, and target table. Subclass of `DatasetSpec`.
* `ThreeDEPCollector` (`collector.py`) — orchestrates collection: `collect(spec, force)` and `generate_ddl(spec)`.

Specs are defined in `loci/sources/dataset_specs.py`, alongside the per-region `BBox` constants (see [Defining specs](#defining-specs)). Raster serialization, tiling, and the ingest loop live in `loci/raster/{wkb,ingest}.py` and are reusable by any raster source.

## Quick start

```python
from loci.collectors.threedep.client import ThreeDEPClient
from loci.collectors.threedep.metadata import ThreeDEPMetadata
from loci.collectors.threedep.collector import ThreeDEPCollector
from loci.collectors.threedep.spec import ThreeDEPSpec
from loci.sources.dataset_specs import PORTLAND_BBOX

spec = ThreeDEPSpec(
    name="portland_3dep_elevation",
    target_table="portland_3dep_elevation",
    bbox=PORTLAND_BBOX,            # product defaults to "13" (~10 m)
)

# Explore: which tiles does this bbox need, and are they all staged?
meta = ThreeDEPMetadata(ThreeDEPClient())
meta.products()                   # {'13': '1/3 arc-second (~10 m)', '1': '...'}
meta.describe(spec.bbox)          # prints needed / available / missing tiles

# Create the table (run the DDL via a migration), then collect
collector = ThreeDEPCollector(engine=engine)
collector.print_ddl(spec)
summary = collector.collect(spec)              # incremental
summary = collector.collect(spec, force=True)  # full refresh
```

## A raster source

The one thing that makes this collector different: the data is a raster, but it still flows through the ordinary `StagedIngest` SCD2 path with **no engine changes**. The mechanism (in `loci/raster/`):

* Each downloaded 1° GeoTIFF is read in windows and split into fixed-size **sub-tiles** (default 512 px). Windowed reads keep peak memory at one sub-tile, so a several-hundred-MB source tile never lands in memory whole.
* Each sub-tile is serialized to a **PostGIS raster hex-WKB** string — the same representation `raster2pgsql` emits — and handed to `StagedIngest` as the value of a `raster` column. PostGIS parses it on COPY exactly as it parses geometry WKT.
* SCD2 keys on `tile_id` (`"<1°-tile>/<row>_<col>"`, stable across runs) with the raster excluded from the record hash; a cheap `checksum` (md5 of the sub-tile's bytes) carries the change signal.

**Prerequisite:** the target database needs the raster extension — `CREATE EXTENSION postgis_raster;` (plain `postgis` does not include it).

## Update semantics

The unit of work is one **1° source tile**. The seamless products are essentially static — a tile changes only when USGS re-stages it — so:

* `collect(spec, force=False)` downloads only 1° tiles that have **no rows yet** in the target, then ingests them. This is the cheap path: it avoids re-downloading tiles already held. Presence is judged by whether any sub-tile of the 1° tile is current in the target.
* `collect(spec, force=True)` re-downloads **every** tile in the bbox and re-ingests. SCD2 dedupes unchanged sub-tiles away and versions any USGS actually changed, so a forced run is how you pick up re-staged tiles.

All ingestion is `StagedIngest` in SCD2 mode, so recollection is always safe: unchanged sub-tiles dedupe away and a forced re-run merges zero rows.

`collect` returns a summary dict: `spec_name`, `mode`, `product`, `tiles_total`, `tiles_collected`, `tiles_skipped_present`, `tiles_missing_at_source`, `sub_tiles_read`, `rows_merged`, and any per-tile `errors` (a failed tile is recorded and the run continues).

## BBox selection and clipping

The `BBox` does double duty. First, the collector enumerates the 1° tiles whose cells intersect it and downloads those. Second, when tiling each downloaded block it **clips** to the bbox, so only sub-tiles covering the region are stored — not the whole 1° block, which would carry up to ~4× the area.

Note the two operate at different granularities: tile *selection* is at 1° resolution (a roughly-right bbox selects the same tiles), while the *clip* is exact.

## DDL generation

`generate_ddl(spec)` produces a raster tile table: `tile_id`, a `rast raster` column, `checksum`, the tile-extent columns (`min_x`/`min_y`/`max_x`/`max_y`), `ingested_at`, the SCD2 metadata columns, a `(tile_id, record_hash)` unique constraint, a partial current-rows index, and — the raster-specific part — a **GiST index on `ST_ConvexHull(rast)`** restricted to current rows, which is what makes point sampling fast. It deliberately does **not** call `AddRasterConstraints`: the strict srid/scale/alignment constraints add friction to SCD2 inserts and aren't needed for `ST_Value`. The collector does not create tables itself — paste the DDL into a migration and apply it.

## Querying elevation

```sql
select ST_Value(rast, ST_SetSRID(ST_Point(:lon, :lat), 4269),
                resample => 'bilinear') as elevation_m
from   raw_data.portland_3dep_elevation r
where  r."valid_to" is null
  and  ST_Intersects(rast, ST_SetSRID(ST_Point(:lon, :lat), 4269));
```

`ST_Intersects(raster, point)` is served by the convex-hull GiST index, so each point resolves to its one sub-tile. `resample => 'bilinear'` interpolates across neighboring cells, smoother than nearest-cell sampling on a 10 m grid. To sample many points at once, join your points table to the raster on `ST_Intersects`. For ad-hoc inspection and rendering see `loci/dev/raster_inspect.py`.

## Tuning

Two knobs, both arguments so they can be swept without touching the modules:

* `tile_size` (collector, default 512) — sub-tile edge in pixels. Larger means fewer, bigger rows (512 keeps a 1° `"13"` tile to ~480 rows vs ~1,800 at 256); smaller means tighter index entries and smaller per-sample reads. 256–512 is the sweet spot for point queries.
* `batch_size` (default 16) — sub-tiles per COPY batch, low because each raster field is large. Trades staging-buffer memory against round-trips.

## Limitations

* **Partial tiles aren't self-healed on incremental runs.** Presence is judged per 1° tile, so a run that crashed partway through a tile looks "present" and is skipped on the next incremental run. Re-run with `force=True` to heal it. We accept this because the source rarely changes and a forced run is cheap to schedule periodically.
* **Bare earth vs digital surface.** 3DEP's seamless products are bare-earth; if you ever switch to a surface model, expect canopy/structure bias in sampled values.
* **No 1 m product.** The project-based 1 m tiling isn't supported by the URL enumeration here; 10 m is ample for most needs.

## Defining specs

Specs live in `loci/sources/dataset_specs.py`, alongside the per-region `BBox` constants (`PORTLAND_BBOX`, `CHICAGO_BBOX`, …). Define one `ThreeDEPSpec` per region you want to cover, landing each in its own table:

```python
PORTLAND_3DEP_SPEC = ThreeDEPSpec(
    name="portland_3dep_elevation",
    target_table="portland_3dep_elevation",
    bbox=PORTLAND_BBOX,
)
```

Because the bbox does double duty (tile selection + clip), reuse the same region `BBox` constant you use for that region's other specs so the stored extent lines up.

## Tests

Three layers, under `tests/collectors/threedep/`:

* **Offline** — tile addressing, spec validation, the bbox clip filter, and collector orchestration with a stub engine + fake client. No network, no database.
* **DB-backed** — fakes the HTTP boundary (`FakeThreeDEPClient` serves synthetic in-extent tiles) and runs ingestion against a real Postgres, so the raster COPY/`ST_Value` path and SCD2 semantics are exercised. Gated on `LOCI_TEST_PG*`.
* **Live** — the only network test: verifies the real `prd-tnm` URL contract and that a real tile parses with the assumed CRS/units. Opt-in via `LOCI_TEST_3DEP_LIVE=1`.

```console
# offline only
uv run pytest platform/tests/collectors/threedep -q
# include the DB-backed tests
uv run --env-file .env_test pytest platform/tests/collectors/threedep -v
# include the live source check
LOCI_TEST_3DEP_LIVE=1 uv run --env-file .env_test pytest platform/tests/collectors/threedep -v
```

The raster tooling itself has its own suite under `tests/raster/` (serializer round-trip, tiling coverage, and a PostGIS smoke test).

## Scheduling

The spec is wrapped in a `DatasetUpdateConfig` in `sources/update_configs.py`, and the 3DEP taskflow (`taskflow.py`) calls `collect(spec, force=…)` based on the schedule. Like OSM and unlike CKAN, it's a two-branch flow — `choose_update_mode` decides full vs. incremental for the run, both feeding `check_ingestion_log` — because 3DEP has a real incremental path (skip tiles already held). Since the source is near-static, a long cadence with an occasional `force=True` run to catch re-stages is the natural schedule.
