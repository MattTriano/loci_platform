"""
Unit behaviors of the 3DEP tooling that need neither a database nor
HTTP: spec validation, tile addressing, and the DDL contract.
"""

from __future__ import annotations

import pytest
from loci.collectors.threedep.client import tile_name, tile_url, tiles_for_bbox
from loci.collectors.threedep.collector import ThreeDEPCollector
from loci.geo import BBox

from ..common import NoopTracker
from .helpers import FakeThreeDEPClient, FakeThreeDEPSource, make_elevation_spec

# ---------------------------------------------------------------------
# Behavior: specs that can't be executed safely fail loudly at
# construction time.
# ---------------------------------------------------------------------


def test_spec_requires_bbox():
    with pytest.raises(ValueError, match="bbox"):
        make_elevation_spec("raw_data", bbox=None)


def test_spec_rejects_unknown_product():
    with pytest.raises(ValueError, match="product"):
        make_elevation_spec("raw_data", product="1m")


def test_spec_rejects_overridden_entity_key():
    with pytest.raises(ValueError, match="fixed"):
        make_elevation_spec("raw_data", entity_key=["foo"])


def test_spec_rejects_non_nad83_bbox():
    with pytest.raises(ValueError, match="NAD83"):
        make_elevation_spec(
            "raw_data",
            bbox=BBox(south=41.7, west=-87.8, north=41.9, east=-87.6, srid=4326),
        )


# ---------------------------------------------------------------------
# Behavior: a BBox maps to the correct staged 1-degree tiles (NW-corner
# naming, zero-padded), at 1-degree selection granularity.
# ---------------------------------------------------------------------


def test_tile_name_is_nw_corner_zero_padded():
    assert tile_name(42, -88) == "n42w088"
    assert tile_name(38, -123) == "n38w123"


def test_tile_name_rejects_eastern_or_southern():
    with pytest.raises(ValueError, match="western"):
        tile_name(42, 5)
    with pytest.raises(ValueError, match="northern"):
        tile_name(-1, -88)


def test_bbox_spanning_two_lat_cells():
    bbox = BBox(south=41.62, west=-87.97, north=42.05, east=-87.5)
    assert tiles_for_bbox(bbox) == ["n42w088", "n43w088"]


def test_bbox_spanning_grid_yields_all_cells():
    bbox = BBox(south=37.2, west=-122.6, north=38.5, east=-121.7)
    assert tiles_for_bbox(bbox) == ["n38w122", "n38w123", "n39w122", "n39w123"]


def test_integer_north_edge_does_not_pull_extra_tile():
    bbox = BBox(south=41.6, west=-87.9, north=42.0, east=-87.5)
    assert tiles_for_bbox(bbox) == ["n42w088"]


def test_tile_url_per_product():
    assert tile_url("n42w088", "13").endswith("/13/TIFF/current/n42w088/USGS_13_n42w088.tif")
    assert tile_url("n42w088", "1").endswith("/1/TIFF/current/n42w088/USGS_1_n42w088.tif")


def test_tile_url_rejects_unsupported_product():
    with pytest.raises(ValueError, match="unsupported product"):
        tile_url("n42w088", "1m")


# ---------------------------------------------------------------------
# Behavior: generated DDL defines a raster table the collector can
# ingest into directly — a raster column, the convex-hull GiST index
# that makes ST_Value sampling fast, the extent/provenance columns, and
# SCD2 uniqueness on (tile_id, record_hash).
# ---------------------------------------------------------------------


def _ddl_collector():
    return ThreeDEPCollector(
        engine=object(),
        client=FakeThreeDEPClient(FakeThreeDEPSource()),
        tracker=NoopTracker(),
    )


def test_ddl_is_raster_table_with_convex_hull_index_and_scd2():
    ddl = _ddl_collector().generate_ddl(make_elevation_spec("raw_data"))

    assert '"rast" raster not null' in ddl
    assert "using gist (ST_ConvexHull(rast))" in ddl
    assert 'unique ("tile_id", "record_hash")' in ddl
    assert 'where "valid_to" is null' in ddl
    for col in ("tile_id", "checksum", "min_x", "max_y", "ingested_at"):
        assert f'"{col}"' in ddl
