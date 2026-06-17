"""
Offline tests for the 3DEP client's tile addressing, the spec, and the
raster bounds-clip filter. Network calls (tile_exists/download_tile) are
not exercised here — those belong in a live smoke test.
"""

from __future__ import annotations

import numpy as np
import pytest
import rasterio
from loci.collectors.threedep.client import (
    tile_name,
    tile_url,
    tiles_for_bbox,
)
from loci.collectors.threedep.spec import ThreeDEPDatasetSpec
from loci.geo import BBox
from loci.raster.ingest import iter_tiles
from rasterio.transform import from_origin

# --------------------------------------------------------------------------
# tile_name
# --------------------------------------------------------------------------


def test_tile_name_zero_pads():
    assert tile_name(42, -88) == "n42w088"
    assert tile_name(7, -9) == "n07w009"
    assert tile_name(38, -123) == "n38w123"


def test_tile_name_rejects_eastern_or_southern():
    with pytest.raises(ValueError, match="western"):
        tile_name(42, 5)
    with pytest.raises(ValueError, match="northern"):
        tile_name(-1, -88)


# --------------------------------------------------------------------------
# tiles_for_bbox
# --------------------------------------------------------------------------


def test_chicago_spans_two_lat_tiles():
    # north 42.05 crosses into the [42,43] cell -> n42 and n43.
    bbox = BBox(south=41.62, west=-87.97, north=42.05, east=-87.5)
    assert tiles_for_bbox(bbox) == ["n42w088", "n43w088"]


def test_detroit_spans_two_lon_tiles():
    bbox = BBox(south=42.24, west=-83.29, north=42.46, east=-82.89)
    assert tiles_for_bbox(bbox) == ["n43w083", "n43w084"]


def test_bbox_spanning_grid_yields_all_cells():
    # lat [37,39): n38,n39 ; lon [-123,-121): w123,w122 -> 4 tiles
    bbox = BBox(south=37.2, west=-122.6, north=38.5, east=-121.7)
    assert tiles_for_bbox(bbox) == [
        "n38w122",
        "n38w123",
        "n39w122",
        "n39w123",
    ]


def test_integer_north_edge_does_not_pull_extra_tile():
    # north exactly 42.0 stays within [41,42] -> only n42, not n43.
    bbox = BBox(south=41.6, west=-87.9, north=42.0, east=-87.5)
    assert tiles_for_bbox(bbox) == ["n42w088"]


def test_single_cell_bbox():
    bbox = BBox(south=41.7, west=-87.8, north=41.9, east=-87.6)
    assert tiles_for_bbox(bbox) == ["n42w088"]


# --------------------------------------------------------------------------
# tile_url
# --------------------------------------------------------------------------


def test_tile_url_for_each_product():
    assert tile_url("n42w088", "13").endswith(
        "/Elevation/13/TIFF/current/n42w088/USGS_13_n42w088.tif"
    )
    assert tile_url("n42w088", "1").endswith("/Elevation/1/TIFF/current/n42w088/USGS_1_n42w088.tif")


def test_tile_url_rejects_unsupported_product():
    with pytest.raises(ValueError, match="unsupported product"):
        tile_url("n42w088", "1m")


# --------------------------------------------------------------------------
# ThreeDEPDatasetSpec
# --------------------------------------------------------------------------


def _bbox():
    return BBox(south=41.62, west=-87.97, north=42.05, east=-87.5)


def test_spec_defaults():
    spec = ThreeDEPDatasetSpec(
        name="chicago_elevation", target_table="chicago_elevation", bbox=_bbox()
    )
    assert spec.source == "3dep"
    assert spec.target_schema == "raw_data"
    assert spec.product == "13"
    assert spec.entity_key == ["tile_id"]
    assert spec.dataset_id == "chicago_elevation"


def test_spec_requires_bbox():
    with pytest.raises(ValueError, match="bbox is required"):
        ThreeDEPDatasetSpec(name="x", target_table="x")


def test_spec_rejects_bad_product():
    with pytest.raises(ValueError, match="product must be"):
        ThreeDEPDatasetSpec(name="x", target_table="x", bbox=_bbox(), product="1m")


def test_spec_rejects_overridden_entity_key():
    with pytest.raises(ValueError, match="fixed to"):
        ThreeDEPDatasetSpec(name="x", target_table="x", bbox=_bbox(), entity_key=["foo"])


def test_spec_rejects_non_nad83_bbox():
    with pytest.raises(ValueError, match="NAD83"):
        ThreeDEPDatasetSpec(
            name="x",
            target_table="x",
            bbox=BBox(south=41.62, west=-87.97, north=42.05, east=-87.5, srid=4326),
        )


# --------------------------------------------------------------------------
# bounds clip filter on iter_tiles
# --------------------------------------------------------------------------


@pytest.fixture
def small_dem(tmp_path):
    # 100x100 at 0.01 deg, origin (-88, 42) north-up -> covers lon [-88,-87], lat [41,42]
    arr = np.arange(100 * 100, dtype=np.float32).reshape(100, 100)
    path = tmp_path / "d.tif"
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=100,
        width=100,
        count=1,
        dtype="float32",
        crs="EPSG:4269",
        transform=from_origin(-88.0, 42.0, 0.01, 0.01),
    ) as d:
        d.write(arr, 1)
    return str(path)


def test_bounds_keeps_only_intersecting_tiles(small_dem):
    all_tiles = list(iter_tiles(small_dem, source_id="d", tile_size=25))
    assert len(all_tiles) == 16  # 4x4 grid of 25px tiles

    # Clip to the top-left quarter of the raster's extent.
    clip = (-88.0, 41.75, -87.75, 42.0)  # min_x, min_y, max_x, max_y
    clipped = list(iter_tiles(small_dem, source_id="d", tile_size=25, bounds=clip))

    assert 0 < len(clipped) < len(all_tiles)
    # Every returned tile actually intersects the clip extent.
    for t in clipped:
        assert not (
            t.max_x < clip[0] or t.min_x > clip[2] or t.max_y < clip[1] or t.min_y > clip[3]
        )


def test_bounds_none_is_unfiltered(small_dem):
    a = list(iter_tiles(small_dem, source_id="d", tile_size=25))
    b = list(iter_tiles(small_dem, source_id="d", tile_size=25, bounds=None))
    assert len(a) == len(b) == 16
