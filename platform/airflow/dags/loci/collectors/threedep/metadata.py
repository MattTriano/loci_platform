# /loci_platform/platform/airflow/dags/loci/collectors/threedep/metadata.py
"""
Source exploration for USGS 3DEP.

Unlike a catalog-backed source (CMS/DKAN) there's no dataset listing to
browse — 3DEP exposes a fixed set of seamless products on a fixed tile
grid. So "explore the source" here means: what products exist, which
tiles a region needs, and which of those are actually staged (so you can
catch ocean/gap tiles before a collect run rather than during it).

Usage:
    meta = ThreeDEPMetadata()
    meta.products()                       # {'13': '...', '1': '...'}
    meta.tiles_for_bbox(spec.bbox)        # ['n42w088', 'n43w088']
    meta.describe(spec.bbox)              # prints a coverage summary
"""

from __future__ import annotations

import logging

from loci.collectors.threedep.client import (
    SUPPORTED_PRODUCTS,
    ThreeDEPClient,
    tiles_for_bbox,
)
from loci.geo import BBox

logger = logging.getLogger(__name__)


class ThreeDEPMetadata:
    """Explore 3DEP products and check tile coverage for a region."""

    def __init__(self, client: ThreeDEPClient | None = None) -> None:
        self.client = client or ThreeDEPClient()

    def products(self) -> dict[str, str]:
        """The seamless products this collector can fetch, code -> description."""
        return dict(SUPPORTED_PRODUCTS)

    def tiles_for_bbox(self, bbox: BBox) -> list[str]:
        """The 1-degree tiles a bbox needs (pure; no network)."""
        return tiles_for_bbox(bbox)

    def coverage(self, bbox: BBox, product: str = "13") -> dict[str, bool]:
        """Map each needed tile to whether it is staged at the source (HEAD checks)."""
        return {name: self.client.tile_exists(name, product) for name in tiles_for_bbox(bbox)}

    def missing(self, bbox: BBox, product: str = "13") -> list[str]:
        """Tiles a bbox needs that are not available at the source."""
        return sorted(n for n, ok in self.coverage(bbox, product).items() if not ok)

    def describe(self, bbox: BBox, product: str = "13") -> dict[str, object]:
        """Print and return a short coverage summary for notebook use."""
        cov = self.coverage(bbox, product)
        present = sorted(n for n, ok in cov.items() if ok)
        missing = sorted(n for n, ok in cov.items() if not ok)

        print(f"3DEP product {product} ({SUPPORTED_PRODUCTS.get(product, '?')})")
        print(f"  tiles needed : {len(cov)}")
        print(f"  available    : {len(present)}  {present}")
        if missing:
            print(f"  MISSING      : {len(missing)} {missing}  (likely ocean/gap)")
        return {"product": product, "needed": len(cov), "present": present, "missing": missing}
