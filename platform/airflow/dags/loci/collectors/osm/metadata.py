"""
OsmMetadata — a lightweight tool for browsing available OpenStreetMap
PBF extracts on Geofabrik's download server from a Jupyter notebook.

Usage:
    from osm_metadata import OsmMetadata
    om = OsmMetadata()

    # What regions are available?
    om.list_regions()                          # continents / top-level
    om.list_regions("north-america")           # subregions of a parent
    om.list_regions("us", keyword="illinois")  # search by keyword

    # Search across all regions
    om.search("illinois")
    om.search("DE", by="iso")

    # Get the PBF download URL
    om.get_download_url("illinois")
    om.get_download_url("germany")
"""

from __future__ import annotations

import logging
from functools import lru_cache

import pandas as pd
import requests

logger = logging.getLogger(__name__)

INDEX_URL = "https://download.geofabrik.de/index-v1-nogeom.json"


class OsmMetadata:
    """
    Browse available OSM PBF extracts on Geofabrik's download server.

    The Geofabrik index is a flat list of regions organized into a
    tree via parent references. Each region has a unique string ID
    (e.g. "illinois", "germany", "asia") and download URLs for
    various formats.
    """

    def __init__(self):
        self._session = requests.Session()

    @lru_cache(maxsize=1)
    def _index(self) -> list[dict]:
        """Fetch and cache the Geofabrik region index."""
        resp = self._session.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return [f["properties"] for f in data.get("features", [])]

    # ------------------------------------------------------------------ #
    #  Public methods
    # ------------------------------------------------------------------ #

    def list_regions(
        self,
        parent: str | None = None,
        keyword: str | None = None,
    ) -> pd.DataFrame:
        """
        List available regions, optionally filtered by parent or keyword.

        Parameters
        ----------
        parent : str, optional
            Region ID to list children of (e.g. "north-america", "us",
            "europe"). If None, returns top-level regions (continents).
        keyword : str, optional
            Case-insensitive substring filter on region name or ID.

        Returns a DataFrame with columns: id, name, parent, iso, pbf_url.
        """
        rows = []
        for region in self._index():
            region_parent = region.get("parent")

            # Filter by parent
            if parent is None:
                if region_parent is not None:
                    continue
            else:
                if region_parent != parent:
                    continue

            # Filter by keyword
            if keyword:
                name = region.get("name", "")
                region_id = region.get("id", "")
                if keyword.lower() not in f"{name} {region_id}".lower():
                    continue

            rows.append(_region_to_row(region))

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("name").reset_index(drop=True)
        return df

    def search(
        self,
        keyword: str,
        by: str = "name",
    ) -> pd.DataFrame:
        """
        Search across all regions.

        Parameters
        ----------
        keyword : str
            Search term.
        by : str
            "name" (default) searches name and ID.
            "iso" searches ISO 3166-1 alpha-2 and ISO 3166-2 codes.

        Returns a DataFrame with columns: id, name, parent, iso, pbf_url.
        """
        rows = []
        for region in self._index():
            if by == "iso":
                codes = region.get("iso3166-1:alpha2", []) + region.get("iso3166-2", [])
                if not any(keyword.upper() == c.upper() for c in codes):
                    continue
            else:
                name = region.get("name", "")
                region_id = region.get("id", "")
                if keyword.lower() not in f"{name} {region_id}".lower():
                    continue

            rows.append(_region_to_row(region))

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("name").reset_index(drop=True)
        return df

    def get_download_url(self, region_id: str, fmt: str = "pbf") -> str:
        """
        Get the download URL for a region.

        Parameters
        ----------
        region_id : str
            Geofabrik region ID (e.g. "illinois", "germany").
        fmt : str
            Format key from the Geofabrik index. Common values:
            "pbf" (default), "shp", "history", "updates".

        Returns the URL string.

        Raises
        ------
        KeyError
            If the region or format is not found.
        """
        for region in self._index():
            if region.get("id") == region_id:
                urls = region.get("urls", {})
                if fmt not in urls:
                    available = list(urls.keys())
                    raise KeyError(
                        f"Format {fmt!r} not available for {region_id!r}. Available: {available}"
                    )
                return urls[fmt]

        raise KeyError(f"Region {region_id!r} not found in Geofabrik index.")

    def get_region(self, region_id: str) -> dict:
        """
        Get the full metadata dict for a region.

        Parameters
        ----------
        region_id : str

        Returns the raw properties dict from the Geofabrik index.
        """
        for region in self._index():
            if region.get("id") == region_id:
                return region
        raise KeyError(f"Region {region_id!r} not found in Geofabrik index.")


def _region_to_row(region: dict) -> dict:
    """Convert a Geofabrik region properties dict to a flat row dict."""
    iso_codes = region.get("iso3166-1:alpha2", []) + region.get("iso3166-2", [])
    urls = region.get("urls", {})
    return {
        "id": region.get("id", ""),
        "name": region.get("name", ""),
        "parent": region.get("parent", ""),
        "iso": ", ".join(iso_codes) if iso_codes else "",
        "pbf_url": urls.get("pbf", ""),
    }
