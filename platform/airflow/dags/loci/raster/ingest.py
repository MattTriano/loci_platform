# /loci_platform/platform/airflow/dags/loci/raster/ingest.py
"""
Ingest a local raster file into a PostGIS `raster` table via StagedIngest.

The flow mirrors the OSM collector: a reader yields row dicts, the
orchestrator batches them into engine.staged_ingest in SCD2 mode. The
only thing that makes this a *raster* ingest is that one column (`rast`)
carries a PostGIS raster hex-WKB string instead of a scalar; PostGIS
parses it on COPY exactly as it parses geometry WKT.

Design choices, in keeping with the rest of the platform:

- We tile in Python with rasterio windowed reads, one tile at a time, so
  peak memory is one tile, not the whole file. This is why download-to-
  file-then-ingest is the right shape: rasterio reads windows from the
  file on disk; we never hold the full raster in memory.

- SCD2 keys on `tile_id` (stable across runs for the same geographic
  tile) and detects change via a cheap `checksum` column (md5 of the
  tile's raw bytes). `rast` itself is excluded from the hash — hashing
  the multi-hundred-KB raster text on every row would be wasteful, and
  the checksum already captures content change.

- generate_ddl is built per source-collector elsewhere (as with OSM and
  CMS); raster_table_ddl is the shared helper those collectors call so
  the column set and the GiST index on ST_ConvexHull(rast) stay
  consistent.

A `raster` table is the natural fit for downstream sampling:

    select n.node_id,
           ST_Value(r.rast, n.geom, resample => 'bilinear') as elevation_m
    from   <raster_table> r
    join   nodes n on ST_Intersects(r.rast, n.geom);

The ST_Intersects is served by the GiST index, so each node resolves to
its one tile.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import numpy as np
import rasterio
from loci.raster.wkb import to_hexwkb
from rasterio.windows import Window

logger = logging.getLogger(__name__)

DEFAULT_TILE_SIZE = 256

# Columns the database fills in (SCD2 bookkeeping); excluded from the
# COPY column list by StagedIngest.
METADATA_COLUMNS: set[str] = {"record_hash", "valid_from", "valid_to"}

# Excluded from the record hash. `rast` is excluded because `checksum`
# already carries the content-change signal far more cheaply than
# hashing the raster text. `ingested_at` is excluded so re-runs don't
# spuriously version every tile.
HASH_EXCLUDE_COLUMNS: set[str] = METADATA_COLUMNS | {"rast", "ingested_at"}


@dataclass(frozen=True)
class RasterTile:
    """One tile read from a raster file, ready to become an ingest row."""

    tile_id: str
    rast_hexwkb: str
    checksum: str
    srid: int
    # Tile extent in the raster's CRS, handy for debugging / sanity joins.
    min_x: float
    min_y: float
    max_x: float
    max_y: float


def iter_tiles(
    path: str,
    *,
    source_id: str,
    band: int = 1,
    tile_size: int = DEFAULT_TILE_SIZE,
    bounds: tuple[float, float, float, float] | None = None,
) -> Iterator[RasterTile]:
    """
    Yield non-overlapping tiles covering the raster at `path`.

    Parameters
    ----------
    path : str
        Local path to a GDAL-readable raster (e.g. a downloaded GeoTIFF
        or COG).
    source_id : str
        Stable identifier for this source file/region, used to build
        tile_id. Must be stable across runs so SCD2 recognizes the same
        geographic tile (e.g. the DEM tile name or "<city>").
    band : int
        1-based band index to read. DEMs are single-band; default 1.
    tile_size : int
        Tile edge in pixels. Edge tiles are smaller. 256 keeps each
        tile's index entry tight without too many rows.
    bounds : tuple[float, float, float, float] | None
        Optional (min_x, min_y, max_x, max_y) clip extent, IN THE
        RASTER'S OWN CRS. Tiles whose extent does not intersect it are
        skipped (and their pixels never read). Use this to keep only the
        sub-tiles covering a city, rather than a whole source block. The
        caller is responsible for expressing the extent in the raster's
        CRS — for 3DEP (EPSG:4269) a NAD83 BBox is already correct.

    Yields
    ------
    RasterTile
        One per tile, row-major over the raster (top-left first).
    """
    with rasterio.open(path) as ds:
        srid = _resolve_srid(ds)
        nodata = ds.nodata

        for row_off in range(0, ds.height, tile_size):
            for col_off in range(0, ds.width, tile_size):
                w = min(tile_size, ds.width - col_off)
                h = min(tile_size, ds.height - row_off)
                window = Window(col_off, row_off, w, h)
                transform = ds.window_transform(window)

                # Tile extent first, so a clipped-out tile costs no read.
                left, top = transform * (0, 0)
                right, bottom = transform * (w, h)
                min_x, max_x = min(left, right), max(left, right)
                min_y, max_y = min(top, bottom), max(top, bottom)

                if bounds is not None and not _intersects((min_x, min_y, max_x, max_y), bounds):
                    continue

                pixels = ds.read(band, window=window)

                # affine: a=scale_x, b=skew_x, c=ip_x, d=skew_y, e=scale_y, f=ip_y
                rast_hexwkb = to_hexwkb(
                    pixels,
                    scale_x=transform.a,
                    scale_y=transform.e,
                    ip_x=transform.c,
                    ip_y=transform.f,
                    srid=srid,
                    nodata=nodata,
                    skew_x=transform.b,
                    skew_y=transform.d,
                )

                # Content checksum over the raw tile bytes + the
                # georeference, so a tile that moves or changes values
                # gets a new SCD2 version.
                checksum = _tile_checksum(pixels, transform, srid)

                yield RasterTile(
                    tile_id=f"{source_id}/{row_off}_{col_off}",
                    rast_hexwkb=rast_hexwkb,
                    checksum=checksum,
                    srid=srid,
                    min_x=min_x,
                    min_y=min_y,
                    max_x=max_x,
                    max_y=max_y,
                )


def _intersects(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> bool:
    """True if two (min_x, min_y, max_x, max_y) extents overlap (touching counts)."""
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def tile_to_row(tile: RasterTile, ingested_at: Any) -> dict[str, Any]:
    """Turn a RasterTile into a staged_ingest row dict."""
    return {
        "tile_id": tile.tile_id,
        "rast": tile.rast_hexwkb,
        "checksum": tile.checksum,
        "srid": tile.srid,
        "min_x": tile.min_x,
        "min_y": tile.min_y,
        "max_x": tile.max_x,
        "max_y": tile.max_y,
        "ingested_at": ingested_at,
    }


def raster_table_ddl(target_schema: str, target_table: str, srid: int) -> str:
    """
    CREATE TABLE plus constraint/index DDL for a raster tile table.

    The shape mirrors the OSM/CMS DDL: data columns, SCD2 metadata
    columns, the (entity_key, record_hash) uniqueness constraint, a
    partial current-rows index, and — the raster-specific part — a GiST
    index on ST_ConvexHull(rast) restricted to current rows, which is
    what makes ST_Intersects(rast, point) sampling fast.

    No AddRasterConstraints call: the strict srid/scale/alignment
    constraints are optional, add friction to SCD2 inserts, and are not
    needed for ST_Value sampling. Add them later if a coverage-level
    invariant becomes useful.
    """
    fqn = f"{target_schema}.{target_table}"
    ek = '"tile_id"'

    lines = [
        f"create table if not exists {fqn} (",
        '    "tile_id" text not null,',
        '    "rast" raster not null,',
        '    "checksum" text not null,',
        '    "srid" integer not null,',
        '    "min_x" double precision,',
        '    "min_y" double precision,',
        '    "max_x" double precision,',
        '    "max_y" double precision,',
        "    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC'),",
        '    "record_hash" text not null,',
        "    \"valid_from\" timestamptz not null default (now() at time zone 'UTC'),",
        '    "valid_to" timestamptz',
        ");",
        "",
        f"alter table {fqn}",
        f"    add constraint uq_{target_table}_entity_hash",
        f'    unique ({ek}, "record_hash");',
        "",
        f"create index if not exists ix_{target_table}_current",
        f"    on {fqn} ({ek})",
        '    where "valid_to" is null;',
        "",
        f"create index if not exists ix_{target_table}_rast",
        f"    on {fqn} using gist (ST_ConvexHull(rast))",
        '    where "valid_to" is null;',
    ]
    return "\n".join(lines)


def ingest_raster_file(
    engine: Any,
    path: str,
    *,
    source_id: str,
    target_schema: str,
    target_table: str,
    ingested_at: Any,
    tile_size: int = DEFAULT_TILE_SIZE,
    batch_size: int = 32,
    bounds: tuple[float, float, float, float] | None = None,
) -> dict[str, int]:
    """
    Tile a local raster and SCD2-merge its tiles into the target table.

    Returns a small summary dict. batch_size is intentionally low: each
    raster tile is a large COPY field, so a handful per batch keeps the
    staging buffer modest. `bounds` (in the raster's CRS) clips the tiles
    to a region of interest; see iter_tiles.
    """
    rows: list[dict[str, Any]] = []
    tiles_read = 0

    with engine.staged_ingest(
        target_table=target_table,
        target_schema=target_schema,
        entity_key=["tile_id"],
        metadata_columns=METADATA_COLUMNS,
        hash_exclude_columns=HASH_EXCLUDE_COLUMNS,
    ) as stager:
        for tile in iter_tiles(path, source_id=source_id, tile_size=tile_size, bounds=bounds):
            rows.append(tile_to_row(tile, ingested_at))
            tiles_read += 1
            if len(rows) >= batch_size:
                stager.write_batch(rows)
                rows = []
        if rows:
            stager.write_batch(rows)

    return {
        "tiles_read": tiles_read,
        "rows_staged": stager.rows_staged,
        "rows_merged": stager.rows_merged,
    }


def _resolve_srid(ds: rasterio.DatasetReader) -> int:
    if ds.crs is None:
        raise ValueError("raster has no CRS; cannot determine srid")
    epsg = ds.crs.to_epsg()
    if epsg is None:
        raise ValueError(f"raster CRS {ds.crs} has no EPSG code; reproject before ingest")
    return int(epsg)


def _tile_checksum(pixels: np.ndarray, transform, srid: int) -> str:
    h = hashlib.md5()
    h.update(np.ascontiguousarray(pixels, dtype="<f4").tobytes())
    h.update(
        repr(
            (transform.a, transform.b, transform.c, transform.d, transform.e, transform.f, srid)
        ).encode()
    )
    return h.hexdigest()
