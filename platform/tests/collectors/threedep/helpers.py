# /loci_platform/platform/tests/collectors/threedep/helpers.py
"""
Fakes and test-data helpers for the 3DEP collector tests.

The boundary that gets faked is HTTP (FakeThreeDEPClient duck-types
ThreeDEPClient); ingestion in the DB-backed tests runs against a real
Postgres so StagedIngest's actual SCD2 semantics and the raster
COPY/ST_Value path are part of the behavior under test.

A "source" here is the set of 1-degree tiles USGS has staged. Each
served tile is a small synthetic GeoTIFF georeferenced to that tile's
true 1-degree extent, with a position ramp offset by a per-tile base
value — so a re-staged tile (new base value) produces different pixels
and a different checksum, exactly as a real re-stage would.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import rasterio
from loci.collectors.threedep.spec import ThreeDEPDatasetSpec
from loci.geo import BBox
from rasterio.transform import from_origin

# Synthetic tile is coarse (a 1-degree cell in PX pixels); SUB_TILE is the
# small sub-tile edge the collector tiles with in tests, so a clip to a
# sub-degree bbox visibly drops sub-tiles.
PX = 120
SUB_TILE = 32
NODATA = -9999.0
SRID = 4269

# A bbox wholly inside one 1-degree cell (n42w088), for clean assertions.
SINGLE_TILE_BBOX = BBox(south=41.7, west=-87.8, north=41.9, east=-87.6)
# A bbox crossing two cells (n42w088 + n43w088).
TWO_TILE_BBOX = BBox(south=41.62, west=-87.97, north=42.05, east=-87.5)

_NAME_RE = re.compile(r"n(\d+)w(\d+)")


def tile_extent(name: str) -> tuple[float, float, float, float]:
    """(west, south, east, north) of a 1-degree tile from its NW-corner name."""
    m = _NAME_RE.fullmatch(name)
    north = int(m.group(1))
    west = -int(m.group(2))
    return west, north - 1, west + 1, north


# ---------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------


class FakeThreeDEPSource:
    """In-memory stand-in for the staged tiles: tile name -> base value."""

    def __init__(self):
        self.tiles: dict[str, float] = {}

    def set_tile(self, name: str, base_value: float = 1000.0) -> None:
        """Stage or re-stage a tile (a new base value == changed pixels)."""
        self.tiles[name] = base_value


class FakeThreeDEPClient:
    """Duck-types ThreeDEPClient against a FakeThreeDEPSource. No HTTP."""

    def __init__(self, source: FakeThreeDEPSource):
        self.source = source
        self.downloaded: list[str] = []
        self.fail_tiles: set[str] = set()

    def tile_exists(self, name: str, product: str = "13") -> bool:
        return name in self.source.tiles

    def download_tile(self, name: str, product: str, dest_path) -> Path:
        if name in self.fail_tiles:
            raise RuntimeError(f"Injected failure for {name}")
        _write_synthetic_tile(dest_path, name, self.source.tiles[name])
        self.downloaded.append(name)
        return Path(dest_path)


def _write_synthetic_tile(dest_path, name: str, base_value: float) -> None:
    west, _south, _east, north = tile_extent(name)
    res = 1.0 / PX
    rows = np.arange(PX).reshape(-1, 1)
    cols = np.arange(PX).reshape(1, -1)
    arr = (base_value + rows + cols).astype("float32")
    with rasterio.open(
        dest_path,
        "w",
        driver="GTiff",
        height=PX,
        width=PX,
        count=1,
        dtype="float32",
        crs=f"EPSG:{SRID}",
        transform=from_origin(west, north, res, res),
        nodata=NODATA,
    ) as dst:
        dst.write(arr, 1)


# ---------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------


def seeded_source(tiles=("n42w088",), base_value: float = 1000.0) -> FakeThreeDEPSource:
    source = FakeThreeDEPSource()
    for name in tiles:
        source.set_tile(name, base_value)
    return source


def make_elevation_spec(
    schema: str, bbox: BBox = SINGLE_TILE_BBOX, **overrides
) -> ThreeDEPDatasetSpec:
    kwargs = dict(
        name="fake_elevation",
        target_table="fake_elevation",
        target_schema=schema,
        bbox=bbox,
    )
    kwargs.update(overrides)
    return ThreeDEPDatasetSpec(**kwargs)
