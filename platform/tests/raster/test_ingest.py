"""
Unit tests for loci.raster.ingest.

Tiling and row/DDL building are exercised against a synthetic GeoTIFF
written to a tmp path. The staged_ingest orchestration is exercised with
a stub engine, so these stay offline — no PostGIS required. The live
COPY/ST_Value behavior is covered in test_raster_postgis.py.
"""

from __future__ import annotations

import re
import struct
from contextlib import contextmanager
from datetime import UTC, datetime

import numpy as np
import pytest
import rasterio
from loci.raster.ingest import (
    HASH_EXCLUDE_COLUMNS,
    ingest_raster_file,
    iter_tiles,
    raster_table_ddl,
    tile_to_row,
)
from rasterio.transform import from_origin

# Synthetic DEM geometry: 600 rows x 500 cols, 10 m, north-up, NAD83.
WIDTH, HEIGHT = 500, 600
ORIGIN_X, ORIGIN_Y, RES = 440000.0, 4640000.0, 10.0
NODATA = -9999.0
SRID = 4269


@pytest.fixture
def dem_path(tmp_path):
    arr = np.arange(HEIGHT * WIDTH, dtype=np.float32).reshape(HEIGHT, WIDTH)
    arr[5, 7] = NODATA
    path = tmp_path / "synthetic.tif"
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=HEIGHT,
        width=WIDTH,
        count=1,
        dtype="float32",
        crs="EPSG:4269",
        transform=from_origin(ORIGIN_X, ORIGIN_Y, RES, RES),
        nodata=NODATA,
    ) as dst:
        dst.write(arr, 1)
    return str(path), arr


def _decode_tile(hexwkb):
    """Pull (pixels, scale_x, scale_y, ip_x, ip_y, srid) from a tile's WKB."""
    b = bytes.fromhex(hexwkb)
    sx, sy, ipx, ipy, _skx, _sky = struct.unpack_from("<dddddd", b, 5)
    (srid,) = struct.unpack_from("<i", b, 53)
    (w,) = struct.unpack_from("<H", b, 57)
    (h,) = struct.unpack_from("<H", b, 59)
    px = np.frombuffer(b, dtype="<f4", count=w * h, offset=66).reshape(h, w)
    return px, sx, sy, ipx, ipy, srid


def _offsets(tile_id):
    row_off, col_off = (int(x) for x in tile_id.split("/")[1].split("_"))
    return row_off, col_off


# --------------------------------------------------------------------------
# Tiling
# --------------------------------------------------------------------------


@pytest.mark.parametrize("tile_size", [64, 128, 256, 512])
def test_tiles_cover_raster_with_exact_reconstruction(dem_path, tile_size):
    path, orig = dem_path
    recon = np.full((HEIGHT, WIDTH), np.nan, dtype=np.float32)
    for t in iter_tiles(path, source_id="testcity", tile_size=tile_size):
        px, *_ = _decode_tile(t.rast_hexwkb)
        r, c = _offsets(t.tile_id)
        h, w = px.shape
        # No overlap: this region hasn't been written yet.
        assert np.isnan(recon[r : r + h, c : c + w]).all()
        recon[r : r + h, c : c + w] = px
    assert not np.isnan(recon).any(), "gap in coverage"
    assert np.array_equal(recon, orig)


def test_tile_count_matches_grid(dem_path):
    path, _ = dem_path
    n = len(list(iter_tiles(path, source_id="c", tile_size=256)))
    cols = -(-WIDTH // 256)  # ceil
    rows = -(-HEIGHT // 256)
    assert n == cols * rows == 6


def test_per_tile_georeference_matches_global_transform(dem_path):
    path, _ = dem_path
    for t in iter_tiles(path, source_id="c", tile_size=256):
        _, sx, sy, ipx, ipy, srid = _decode_tile(t.rast_hexwkb)
        r, c = _offsets(t.tile_id)
        assert sx == RES and sy == -RES
        assert ipx == pytest.approx(ORIGIN_X + c * RES)
        assert ipy == pytest.approx(ORIGIN_Y - r * RES)
        assert srid == SRID


def test_edge_tiles_are_smaller(dem_path):
    path, _ = dem_path
    sizes = {}
    for t in iter_tiles(path, source_id="c", tile_size=256):
        px, *_ = _decode_tile(t.rast_hexwkb)
        sizes[_offsets(t.tile_id)] = px.shape
    # 500 wide -> last column tile is 500-256-256 = ... cols at 0,256 -> widths 256,244
    assert sizes[(0, 0)] == (256, 256)
    assert sizes[(0, 256)] == (256, 244)  # right edge
    assert sizes[(512, 0)] == (88, 256)  # bottom edge (600-512)
    assert sizes[(512, 256)] == (88, 244)  # corner


def test_tile_bounds_bracket_the_tile(dem_path):
    path, _ = dem_path
    for t in iter_tiles(path, source_id="c", tile_size=256):
        assert t.min_x < t.max_x
        assert t.min_y < t.max_y


# --------------------------------------------------------------------------
# Checksum (the SCD2 change signal)
# --------------------------------------------------------------------------


def test_checksums_are_stable_across_reads(dem_path):
    path, _ = dem_path
    a = [t.checksum for t in iter_tiles(path, source_id="c", tile_size=256)]
    b = [t.checksum for t in iter_tiles(path, source_id="c", tile_size=256)]
    assert a == b


def test_checksum_changes_when_a_pixel_changes(tmp_path):
    def write(value):
        arr = np.zeros((10, 10), dtype=np.float32)
        arr[0, 0] = value
        p = tmp_path / f"d{value}.tif"
        with rasterio.open(
            p,
            "w",
            driver="GTiff",
            height=10,
            width=10,
            count=1,
            dtype="float32",
            crs="EPSG:4269",
            transform=from_origin(0, 100, 10, 10),
        ) as d:
            d.write(arr, 1)
        return next(iter_tiles(str(p), source_id="c")).checksum

    assert write(1.0) != write(2.0)


# --------------------------------------------------------------------------
# Row schema vs DDL  (guards against drift between the two)
# --------------------------------------------------------------------------

DB_MANAGED = {"record_hash", "valid_from", "valid_to"}


def test_row_keys_match_ddl_data_columns(dem_path):
    path, _ = dem_path
    tile = next(iter_tiles(path, source_id="c", tile_size=256))
    row = tile_to_row(tile, ingested_at=datetime.now(UTC))

    ddl = raster_table_ddl("raw_data", "c_elevation", SRID)
    # Column names are the first quoted token on each definition line.
    ddl_cols = set(re.findall(r'^\s*"([a-z_]+)"', ddl, flags=re.MULTILINE))
    data_cols = ddl_cols - DB_MANAGED

    assert set(row.keys()) == data_cols


def test_ddl_has_raster_column_and_convex_hull_index():
    ddl = raster_table_ddl("raw_data", "toronto_elevation", 3979)
    assert '"rast" raster not null' in ddl
    assert "using gist (ST_ConvexHull(rast))" in ddl
    assert 'unique ("tile_id", "record_hash")' in ddl
    assert 'where "valid_to" is null' in ddl


# --------------------------------------------------------------------------
# Orchestration  (stub engine — verifies SCD2 wiring without a database)
# --------------------------------------------------------------------------


class _StubStager:
    def __init__(self, **kw):
        self.kw = kw
        self.batch_sizes = []
        self.rows_staged = 0
        self.rows_merged = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.rows_merged = self.rows_staged
        return False

    def write_batch(self, rows):
        self.batch_sizes.append(len(rows))
        self.rows_staged += len(rows)


class _StubEngine:
    def __init__(self):
        self.last = None

    @contextmanager
    def staged_ingest(self, **kw):
        self.last = _StubStager(**kw)
        yield self.last


def test_ingest_passes_scd2_config(dem_path):
    path, _ = dem_path
    eng = _StubEngine()
    ingest_raster_file(
        eng,
        path,
        source_id="c",
        target_schema="raw_data",
        target_table="c_elevation",
        ingested_at=datetime.now(UTC),
    )
    assert eng.last.kw["entity_key"] == ["tile_id"]
    assert eng.last.kw["hash_exclude_columns"] == HASH_EXCLUDE_COLUMNS
    assert "rast" in eng.last.kw["hash_exclude_columns"]


def test_ingest_batches_and_counts(dem_path):
    path, _ = dem_path
    eng = _StubEngine()
    summary = ingest_raster_file(
        eng,
        path,
        source_id="c",
        target_schema="raw_data",
        target_table="c_elevation",
        ingested_at=datetime.now(UTC),
        tile_size=256,
        batch_size=4,
    )
    assert summary["tiles_read"] == 6
    assert summary["rows_staged"] == 6
    assert eng.last.batch_sizes == [4, 2]  # 6 tiles, batch of 4


# --------------------------------------------------------------------------
# CRS error paths
# --------------------------------------------------------------------------


def test_missing_crs_raises(tmp_path):
    p = tmp_path / "nocrs.tif"
    with rasterio.open(
        p,
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="float32",
        transform=from_origin(0, 10, 1, 1),
    ) as d:
        d.write(np.zeros((4, 4), dtype="float32"), 1)
    with pytest.raises(ValueError, match="no CRS"):
        list(iter_tiles(str(p), source_id="c"))
