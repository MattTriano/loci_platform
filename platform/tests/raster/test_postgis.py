"""
Integration smoke test for raster ingestion against a real PostGIS.

This is the test that de-risks the two things the offline suite can't:
  1. that COPY of the hex-WKB into a `raster` column parses cleanly
     (i.e. StagedIngest can carry a raster the way it carries geometry),
  2. that the stored tiles answer ST_Value at known coordinates with the
     right elevations, including NULL at nodata pixels.
It also checks the SCD2 behavior: a re-run is a no-op, and changing a
tile creates a new version while closing the old one.

It is SKIPPED unless a database is configured. Point it at a dev/test
PostGIS that has the postgis and postgis_raster extensions available:

    export DWH_TEST_PGHOST=localhost
    export DWH_TEST_PGPORT=5432
    export DWH_TEST_PGDB=loci_test
    export DWH_TEST_PGUSER=postgres
    export DWH_TEST_PGPASSWORD=postgres
    pytest test_raster_postgis.py -v

The test creates and drops its own table, so it won't touch your data.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np
import pytest
import rasterio
from loci.raster.ingest import ingest_raster_file, raster_table_ddl
from rasterio.transform import from_origin

# Small synthetic DEM: 300x300 at 10 m, north-up, NAD83. Pixel (r,c) holds
# value r*WIDTH + c, so any sampled elevation is predictable. One nodata.
WIDTH = HEIGHT = 300
ORIGIN_X, ORIGIN_Y, RES = 440000.0, 4640000.0, 10.0
NODATA = -9999.0
SRID = 4269
NODATA_RC = (40, 70)

SCHEMA = "public"
TABLE = "_raster_ingest_smoketest"


def _env_creds():
    host = os.environ.get("DWH_TEST_PGHOST")
    if not host:
        return None

    @dataclass
    class Creds:
        host: str
        port: int
        database: str
        username: str
        password: str

    return Creds(
        host=host,
        port=int(os.environ.get("DWH_TEST_PGPORT", "5432")),
        database=os.environ.get("DWH_TEST_PGDB", "postgres"),
        username=os.environ.get("DWH_TEST_PGUSER", "postgres"),
        password=os.environ.get("DWH_TEST_PGPASSWORD", ""),
    )


pytestmark = pytest.mark.skipif(
    _env_creds() is None,
    reason="set DWH_TEST_PGHOST (and friends) to run the PostGIS integration test",
)


def _make_dem(path, *, mutate_pixel=None):
    arr = np.arange(HEIGHT * WIDTH, dtype=np.float32).reshape(HEIGHT, WIDTH)
    arr[NODATA_RC] = NODATA
    if mutate_pixel is not None:
        (r, c), v = mutate_pixel
        arr[r, c] = v
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=HEIGHT,
        width=WIDTH,
        count=1,
        dtype="float32",
        crs=f"EPSG:{SRID}",
        transform=from_origin(ORIGIN_X, ORIGIN_Y, RES, RES),
        nodata=NODATA,
    ) as dst:
        dst.write(arr, 1)
    return arr


def _pixel_center(r, c):
    """World (x, y) at the center of pixel (row, col) for a north-up raster."""
    x = ORIGIN_X + (c + 0.5) * RES
    y = ORIGIN_Y - (r + 0.5) * RES
    return x, y


def _sample(engine, x, y):
    """ST_Value at a point, via the convex-hull index join. Returns float|None."""
    df = engine.query(
        f"""
        select ST_Value(r.rast, ST_SetSRID(ST_Point(%(x)s, %(y)s), %(srid)s)) as elev
        from {SCHEMA}.{TABLE} r
        where r."valid_to" is null
          and ST_Intersects(r.rast, ST_SetSRID(ST_Point(%(x)s, %(y)s), %(srid)s))
        """,
        {"x": x, "y": y, "srid": SRID},
    )
    if df.empty:
        return None
    return df.iloc[0]["elev"]


@pytest.fixture
def engine():
    from loci.db.core import PostgresEngine  # platform import

    eng = PostgresEngine(creds=_env_creds())
    # eng.execute("create extension if not exists postgis;")
    # eng.execute("create extension if not exists postgis_raster;")
    eng.execute(f"drop table if exists {SCHEMA}.{TABLE};")
    eng.execute(raster_table_ddl(SCHEMA, TABLE, SRID))
    try:
        yield eng
    finally:
        eng.execute(f"drop table if exists {SCHEMA}.{TABLE};")
        eng.close()


def _current_count(engine):
    df = engine.query(f'select count(*) as n from {SCHEMA}.{TABLE} where "valid_to" is null')
    return int(df.iloc[0]["n"])


def test_copy_parse_and_value_sampling(engine, tmp_path):
    """The core smoke test: ingest parses, and ST_Value returns the right values."""
    path = str(tmp_path / "dem.tif")
    arr = _make_dem(path)

    summary = ingest_raster_file(
        engine,
        path,
        source_id="smoke",
        target_schema=SCHEMA,
        target_table=TABLE,
        ingested_at=datetime.now(UTC),
        tile_size=256,
    )
    assert summary["rows_merged"] == summary["tiles_read"] > 0

    # Sample several known pixels (including across tile boundaries).
    for r, c in [(0, 0), (10, 10), (255, 255), (256, 256), (299, 299)]:
        x, y = _pixel_center(r, c)
        got = _sample(engine, x, y)
        assert got == pytest.approx(float(arr[r, c])), f"pixel ({r},{c})"


def test_nodata_samples_as_null(engine, tmp_path):
    path = str(tmp_path / "dem.tif")
    _make_dem(path)
    ingest_raster_file(
        engine,
        path,
        source_id="smoke",
        target_schema=SCHEMA,
        target_table=TABLE,
        ingested_at=datetime.now(UTC),
    )
    x, y = _pixel_center(*NODATA_RC)
    assert _sample(engine, x, y) is None


def test_reingest_is_a_noop(engine, tmp_path):
    path = str(tmp_path / "dem.tif")
    _make_dem(path)
    first = ingest_raster_file(
        engine,
        path,
        source_id="smoke",
        target_schema=SCHEMA,
        target_table=TABLE,
        ingested_at=datetime.now(UTC),
    )
    second = ingest_raster_file(
        engine,
        path,
        source_id="smoke",
        target_schema=SCHEMA,
        target_table=TABLE,
        ingested_at=datetime.now(UTC),
    )
    assert first["rows_merged"] > 0
    assert second["rows_merged"] == 0  # all tiles deduped on unchanged checksum
    assert _current_count(engine) == first["tiles_read"]


def test_changed_tile_creates_new_version(engine, tmp_path):
    path = str(tmp_path / "dem.tif")
    _make_dem(path)
    first = ingest_raster_file(
        engine,
        path,
        source_id="smoke",
        target_schema=SCHEMA,
        target_table=TABLE,
        ingested_at=datetime.now(UTC),
    )

    # Change one pixel in the top-left tile and re-ingest.
    _make_dem(path, mutate_pixel=((1, 1), 12345.0))
    second = ingest_raster_file(
        engine,
        path,
        source_id="smoke",
        target_schema=SCHEMA,
        target_table=TABLE,
        ingested_at=datetime.now(UTC),
    )

    assert second["rows_merged"] == 1  # exactly the one changed tile
    assert _current_count(engine) == first["tiles_read"]  # still one current per tile

    # The new value is what ST_Value now returns; total row count grew by one.
    x, y = _pixel_center(1, 1)
    assert _sample(engine, x, y) == pytest.approx(12345.0)
    total = engine.query(f"select count(*) as n from {SCHEMA}.{TABLE}").iloc[0]["n"]
    assert int(total) == first["tiles_read"] + 1
