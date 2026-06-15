# /loci_platform/platform/airflow/dags/loci/collectors/threedep/spec.py
"""
Dataset specification for USGS 3DEP elevation collection.

A ThreeDEPDatasetSpec declares which region to collect (a BBox), which seamless
product, and where to land it. One instance per city; the raster lands
in a per-city table, mirroring the per-city OSM raw tables. SCD2 keying
is fixed to ["tile_id"] — the raster ingestion path keys every sub-tile
on its tile_id, so this is not a caller choice.

Example:
    spec = ThreeDEPDatasetSpec(
        name="chicago_elevation",
        target_table="chicago_elevation",
        bbox=BBox(south=41.62, west=-87.97, north=42.05, east=-87.5),
        product="13",
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loci.collectors.base_spec import DatasetSpec
from loci.collectors.threedep.client import SUPPORTED_PRODUCTS
from loci.geo import BBox

# Native SRID / vertical units of the seamless 3DEP products.
THREEDEP_SRID = 4269  # NAD83 geographic

_RASTER_ENTITY_KEY = ["tile_id"]


@dataclass
class ThreeDEPDatasetSpec(DatasetSpec):
    """
    Specification for a 3DEP elevation dataset.

    Parameters
    ----------
    name : str
        Human-readable identifier, conventionally equal to target_table
        (e.g. "chicago_elevation").
    target_table : str
        Per-city destination table name.
    target_schema : str
        Destination schema. Default "raw_data".
    bbox : BBox
        Region to collect. Tiles intersecting it are fetched, and stored
        sub-tiles are clipped to it. Expected in NAD83 (EPSG:4269) so it
        matches the tiles' CRS; the BBox default SRID already is.
    product : str
        Seamless product code: "13" (1/3 arc-second, ~10 m; default) or
        "1" (1 arc-second, ~30 m).
    entity_key : list[str]
        Fixed to ["tile_id"]; overriding raises.
    """

    source: str = field(default="3dep", init=False)
    name: str = ""
    target_table: str = ""
    target_schema: str = "raw_data"
    bbox: BBox | None = None
    product: str = "13"
    entity_key: list[str] = field(default_factory=lambda: list(_RASTER_ENTITY_KEY))

    @property
    def dataset_id(self) -> str:
        return self.target_table

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name is required")
        if not self.target_table:
            raise ValueError("target_table is required")
        if not self.target_schema:
            raise ValueError("target_schema is required")
        if self.bbox is None:
            raise ValueError("bbox is required")
        if self.product not in SUPPORTED_PRODUCTS:
            raise ValueError(
                f"product must be one of {sorted(SUPPORTED_PRODUCTS)}, got {self.product!r}"
            )
        if self.entity_key != _RASTER_ENTITY_KEY:
            raise ValueError(
                f"entity_key for raster collection is fixed to {_RASTER_ENTITY_KEY}; "
                f"got {self.entity_key}"
            )
        if self.bbox.srid != THREEDEP_SRID:
            # Not fatal — the collector could transform — but for 3DEP the
            # tiles are 4269 and a mismatched bbox would mis-clip, so flag it.
            raise ValueError(
                f"bbox.srid is {self.bbox.srid}; 3DEP tiles are EPSG:{THREEDEP_SRID}. "
                f"Provide the bbox in NAD83 so tile selection and clipping line up."
            )
