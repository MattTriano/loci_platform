# /loci_platform/platform/airflow/dags/loci/collectors/threedep/client.py
"""
HTTP client and tile addressing for USGS 3DEP seamless DEMs.

3DEP's seamless products are pre-staged as 1 degree x 1 degree GeoTIFF
tiles at predictable URLs under the public TNM S3 bucket, e.g.

    https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/
        current/n42w088/USGS_13_n42w088.tif

Tiles are named by their NORTHWEST corner (nNNwWWW), are in NAD83
(EPSG:4269), and elevations are NAVD88 meters. Because they sit at
predictable URLs we don't need the TNM Access API: given a BBox we
enumerate the 1 degree tiles it touches and build URLs directly. This
mirrors the OverpassAPIQuery.for_bbox pattern — geometry in, concrete
addresses out.

Products supported here are the two seamless, predictably-tiled layers:
    "13" -> 1/3 arc-second (~10 m)   [default; right for street grade]
    "1"  -> 1 arc-second   (~30 m)
The 1 m product uses a different, project-based tiling and is out of
scope for this enumeration.

Usage:
    client = ThreeDEPClient()
    tiles = tiles_for_bbox(spec.bbox)              # ['n42w088', 'n43w088']
    for name in tiles:
        if client.tile_exists(name, product="13"):
            client.download_tile(name, "13", dest_path)
"""

from __future__ import annotations

import logging
import math
from pathlib import Path

import requests
from loci.geo import BBox
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation"

# product code -> (path segment, filename prefix). Both seamless layers
# share the nNNwWWW tiling; only the code in the path and filename differ.
SUPPORTED_PRODUCTS: dict[str, str] = {
    "13": "1/3 arc-second (~10 m)",
    "1": "1 arc-second (~30 m)",
}


# ----------------------------------------------------------------------
# Tile addressing (pure; no network)
# ----------------------------------------------------------------------


def tile_name(north_edge: int, west_edge: int) -> str:
    """
    Name the 1 degree tile by its NW corner.

    Parameters
    ----------
    north_edge : int
        Latitude of the tile's north edge (the cell covers
        [north_edge - 1, north_edge]). Positive (northern hemisphere).
    west_edge : int
        Longitude of the tile's west edge (the cell covers
        [west_edge, west_edge + 1]). Non-positive (western hemisphere).

    Returns
    -------
    str
        e.g. "n42w088".
    """
    if north_edge <= 0:
        raise ValueError(f"north_edge must be positive (northern hemisphere); got {north_edge}")
    if west_edge > 0:
        raise ValueError(
            f"west_edge must be <= 0 (western hemisphere; 3DEP CONUS naming); got {west_edge}"
        )
    return f"n{north_edge:02d}w{-west_edge:03d}"


def tiles_for_bbox(bbox: BBox) -> list[str]:
    """
    Enumerate the 1 degree tiles whose cells intersect `bbox`.

    A latitude cell [k, k+1] has north edge k+1; a longitude cell
    [m, m+1] has west edge m. We walk every cell the box touches. The
    result is sorted and de-duplicated.
    """
    names = []
    for k in range(math.floor(bbox.south), math.ceil(bbox.north)):
        north_edge = k + 1
        for m in range(math.floor(bbox.west), math.ceil(bbox.east)):
            names.append(tile_name(north_edge, west_edge=m))
    return sorted(set(names))


def tile_url(name: str, product: str = "13", base_url: str = DEFAULT_BASE_URL) -> str:
    """Build the `current` GeoTIFF URL for a tile + product."""
    _check_product(product)
    return f"{base_url}/{product}/TIFF/current/{name}/USGS_{product}_{name}.tif"


def _check_product(product: str) -> None:
    if product not in SUPPORTED_PRODUCTS:
        raise ValueError(
            f"unsupported product {product!r}; supported: {sorted(SUPPORTED_PRODUCTS)}"
        )


# ----------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------


class ThreeDEPClient:
    """
    Thin client over the public TNM staged-products bucket.

    No auth. Mirrors CMSClient's session + urllib3 Retry for streamed
    downloads. The bucket is public, so a missing tile (ocean, gap)
    answers a non-200 to HEAD rather than raising.

    Parameters
    ----------
    base_url : str
        Root of the staged Elevation products.
    timeout : int
        Per-request timeout in seconds. DEM tiles are large; default 120.
    """

    def __init__(self, base_url: str = DEFAULT_BASE_URL, timeout: int = 120) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.logger = logging.getLogger("threedep_client")

        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def tile_url(self, name: str, product: str = "13") -> str:
        return tile_url(name, product=product, base_url=self.base_url)

    def tile_exists(self, name: str, product: str = "13") -> bool:
        """HEAD the tile; True iff it is present (200)."""
        url = self.tile_url(name, product=product)
        resp = self.session.head(url, timeout=self.timeout, allow_redirects=True)
        if resp.status_code == 200:
            return True
        if resp.status_code in (403, 404):
            return False
        resp.raise_for_status()
        return False

    def download_tile(self, name: str, product: str, dest_path: str | Path) -> Path:
        """
        Stream a tile's GeoTIFF to dest_path and return the path.

        Raises for HTTP errors other than the caller-checked existence.
        """
        url = self.tile_url(name, product=product)
        dest_path = Path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger.info("Downloading %s", url)
        with self.session.get(url, stream=True, timeout=self.timeout) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        self.logger.info("Downloaded %s (%.1f MB)", name, dest_path.stat().st_size / (1024 * 1024))
        return dest_path
