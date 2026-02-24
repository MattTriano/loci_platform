"""
TigerMetadata — a lightweight tool for exploring Census Bureau TIGER/Line
shapefiles and Cartographic Boundary files from a Jupyter notebook.

Usage:
    from tiger_metadata import TigerMetadata
    tm = TigerMetadata()

    # What vintages are available?
    tm.list_vintages()                              # TIGER/Line vintages
    tm.list_vintages(source="cartographic")         # Cartographic boundary vintages

    # What layer types are available for a vintage?
    tm.list_layers(2024)                            # all TIGER/Line layers
    tm.list_layers(2024, keyword="road")            # search by keyword
    tm.list_layers(2024, source="cartographic")     # cartographic boundary layers

    # What files are available for a layer?
    tm.list_files(2024, "TRACT")                    # state-based tract shapefiles
    tm.list_files(2024, "PRIMARYROADS")             # national roads shapefile
    tm.list_files(2024, "tract", source="cartographic")  # cartographic boundaries

    # Build a download URL
    tm.get_download_url(2024, "TRACT", state_fips="17")
    tm.get_download_url(2024, "tract", source="cartographic", state_fips="17")
"""

from __future__ import annotations

import logging
import re

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# File naming conventions:
# TIGER/Line:  tl_{year}_{fips}_{layer}.zip  (state-based)
#              tl_{year}_us_{layer}.zip       (national)
# Cartographic: cb_{year}_{fips}_{layer}_{resolution}.zip  (state-based)
#               cb_{year}_us_{layer}_{resolution}.zip       (national)

# Human-readable descriptions for TIGER/Line layer directory names.
# This covers the layers available in recent vintages (2020+).
TIGER_LAYER_DESCRIPTIONS = {
    "ADDR": "Address Ranges (county-based)",
    "ADDRFEAT": "Address Range Feature (county-based)",
    "ADDRFN": "Address Range-Feature Name Relationship (county-based)",
    "AIANNH": "American Indian / Alaska Native / Native Hawaiian Areas",
    "AITSN": "American Indian Tribal Subdivision (national)",
    "ANRC": "Alaska Native Regional Corporation",
    "AREALM": "Area Landmarks (state-based)",
    "AREAWATER": "Area Hydrography (county-based)",
    "BG": "Block Groups (state-based)",
    "CBSA": "Core Based Statistical Areas (national)",
    "CD": "Congressional Districts (national)",
    "COASTLINE": "Coastline (national)",
    "CONCITY": "Consolidated Cities (state-based)",
    "COUNTY": "Counties and Equivalent Entities (national)",
    "COUSUB": "County Subdivisions (state-based)",
    "CSA": "Combined Statistical Areas (national)",
    "EDGES": "All Lines / Edges (county-based)",
    "ELSD": "Elementary School Districts (state-based)",
    "ESTATE": "Estates (U.S. Virgin Islands only)",
    "FACES": "Topological Faces with Geocodes (county-based)",
    "FACESAH": "Faces-Area Hydrography Relationship (county-based)",
    "FACESAL": "Faces-Area Landmark Relationship (county-based)",
    "FACESMIL": "Faces-Military Installation Relationship (county-based)",
    "FEATNAMES": "Feature Names Relationship (county-based)",
    "INTERNATIONALBOUNDARY": "International Boundary (national)",
    "LINEARWATER": "Linear Hydrography (county-based)",
    "METDIV": "Metropolitan Divisions (national)",
    "MIL": "Military Installations (national)",
    "PLACE": "Places / Census Designated Places (state-based)",
    "POINTLM": "Point Landmarks (state-based)",
    "PRIMARYROADS": "Primary Roads (national)",
    "PRISECROADS": "Primary and Secondary Roads (state-based)",
    "PUMA20": "Public Use Microdata Areas (2020, state-based)",
    "RAILS": "Railroads (national)",
    "ROADS": "All Roads (county-based)",
    "SCSD": "Secondary School Districts (state-based)",
    "SDADM": "School District Administration (state-based)",
    "SLDL": "State Legislative Districts - Lower Chamber (state-based)",
    "SLDU": "State Legislative Districts - Upper Chamber (state-based)",
    "STATE": "States and Equivalent Entities (national)",
    "SUBBARRIO": "Subbarrios (Puerto Rico only)",
    "TABBLOCK20": "Census Blocks (2020, state-based)",
    "TABBLOCKSUFX": "Census Blocks with Suffixes (state-based)",
    "TBG": "Tribal Block Groups (national)",
    "TRACT": "Census Tracts (state-based)",
    "TTRACT": "Tribal Census Tracts (national)",
    "UAC20": "Urban Areas (2020, national)",
    "UNSD": "Unified School Districts (state-based)",
    "ZCTA520": "ZIP Code Tabulation Areas (2020, national)",
}

# Scope of each layer: "state", "county", or "national"
TIGER_LAYER_SCOPE = {
    "ADDR": "county",
    "ADDRFEAT": "county",
    "ADDRFN": "county",
    "AIANNH": "national",
    "AITSN": "national",
    "ANRC": "state",
    "AREALM": "state",
    "AREAWATER": "county",
    "BG": "state",
    "CBSA": "national",
    "CD": "national",
    "COASTLINE": "national",
    "CONCITY": "state",
    "COUNTY": "national",
    "COUSUB": "state",
    "CSA": "national",
    "EDGES": "county",
    "ELSD": "state",
    "ESTATE": "state",
    "FACES": "county",
    "FACESAH": "county",
    "FACESAL": "county",
    "FACESMIL": "county",
    "FEATNAMES": "county",
    "INTERNATIONALBOUNDARY": "national",
    "LINEARWATER": "county",
    "METDIV": "national",
    "MIL": "national",
    "PLACE": "state",
    "POINTLM": "state",
    "PRIMARYROADS": "national",
    "PRISECROADS": "state",
    "PUMA20": "state",
    "RAILS": "national",
    "ROADS": "county",
    "SCSD": "state",
    "SDADM": "state",
    "SLDL": "state",
    "SLDU": "state",
    "STATE": "national",
    "SUBBARRIO": "state",
    "TABBLOCK20": "state",
    "TABBLOCKSUFX": "state",
    "TBG": "national",
    "TRACT": "state",
    "TTRACT": "national",
    "UAC20": "national",
    "UNSD": "state",
    "ZCTA520": "national",
}

# Cartographic boundary layers use lowercase names in URLs.
# Mapping from cb filename layer codes to descriptions.
CARTOGRAPHIC_LAYER_DESCRIPTIONS = {
    "aiannh": "American Indian / Alaska Native / Native Hawaiian Areas",
    "aitsn": "American Indian Tribal Subdivision",
    "anrc": "Alaska Native Regional Corporation",
    "bg": "Block Groups",
    "cbsa": "Core Based Statistical Areas",
    "cd": "Congressional Districts",
    "concity": "Consolidated Cities",
    "county": "Counties and Equivalent Entities",
    "cousub": "County Subdivisions",
    "csa": "Combined Statistical Areas",
    "elsd": "Elementary School Districts",
    "estate": "Estates (U.S. Virgin Islands)",
    "metdiv": "Metropolitan Divisions",
    "place": "Places / Census Designated Places",
    "puma20": "Public Use Microdata Areas (2020)",
    "scsd": "Secondary School Districts",
    "sldl": "State Legislative Districts - Lower Chamber",
    "sldu": "State Legislative Districts - Upper Chamber",
    "state": "States and Equivalent Entities",
    "subbarrio": "Subbarrios (Puerto Rico)",
    "tbg": "Tribal Block Groups",
    "tract": "Census Tracts",
    "ttract": "Tribal Census Tracts",
    "ua20": "Urban Areas (2020)",
    "unsd": "Unified School Districts",
    "zcta520": "ZIP Code Tabulation Areas (2020)",
}


class TigerMetadata:
    """
    Browse available Census TIGER/Line and Cartographic Boundary files.

    Two sources are supported:
        - "tiger" (default): Full-detail TIGER/Line shapefiles. Includes
          everything: boundaries, roads, water, rails, landmarks, etc.
        - "cartographic": Simplified/coastline-clipped boundary files.
          Smaller files, fewer layer types, available as shapefiles.
    """

    BASE = "https://www2.census.gov/geo/tiger"

    def __init__(self):
        self._session = requests.Session()
        self.logger = logging.getLogger("tiger_metadata")

    def _fetch_directory(self, url: str) -> str:
        """Fetch an HTML directory listing."""
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def _parse_directory_links(html: str) -> list[dict]:
        """
        Parse an Apache-style directory listing into a list of entries.

        Returns dicts with keys: name, date, size.
        """
        entries = []
        # Match table rows with links: name, last modified, size
        # Apache directory listing format: <a href="NAME">NAME</a> DATE SIZE
        for match in re.finditer(
            r'<a href="([^"]+)">\1</a>\s+'
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+"
            r"([\d.]+[KMG]?|-)",
            html,
        ):
            name, date, size = match.groups()
            entries.append({"name": name, "date": date, "size": size})
        return entries

    # ------------------------------------------------------------------ #
    #  Vintages
    # ------------------------------------------------------------------ #

    def list_vintages(self, source: str = "tiger") -> pd.DataFrame:
        """
        List available vintages (years).

        Parameters
        ----------
        source : str
            "tiger" for TIGER/Line shapefiles, "cartographic" for
            Cartographic Boundary files.

        Returns a DataFrame with columns: vintage, directory, source.
        """
        html = self._fetch_directory(self.BASE)
        rows = []

        if source == "tiger":
            for match in re.finditer(r'<a href="(TIGER(\d{4})/)">', html):
                dirname, year = match.groups()
                rows.append(
                    {
                        "vintage": int(year),
                        "directory": dirname,
                        "source": "tiger",
                    }
                )
        elif source == "cartographic":
            for match in re.finditer(r'<a href="(GENZ(\d{4})/)">', html):
                dirname, year = match.groups()
                rows.append(
                    {
                        "vintage": int(year),
                        "directory": dirname,
                        "source": "cartographic",
                    }
                )
        else:
            raise ValueError(f"Unknown source {source!r}. Use 'tiger' or 'cartographic'.")

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("vintage").reset_index(drop=True)
        return df

    # ------------------------------------------------------------------ #
    #  Layers
    # ------------------------------------------------------------------ #

    def list_layers(
        self,
        vintage: int,
        source: str = "tiger",
        keyword: str | None = None,
    ) -> pd.DataFrame:
        """
        List available layers (geography/feature types) for a vintage.

        Parameters
        ----------
        vintage : int
        source : str
            "tiger" or "cartographic".
        keyword : str, optional
            Case-insensitive filter on layer name or description.

        Returns a DataFrame with columns: layer, description, scope.
        """
        if source == "tiger":
            return self._list_tiger_layers(vintage, keyword)
        elif source == "cartographic":
            return self._list_cartographic_layers(vintage, keyword)
        else:
            raise ValueError(f"Unknown source {source!r}.")

    def _list_tiger_layers(self, vintage: int, keyword: str | None = None) -> pd.DataFrame:
        url = f"{self.BASE}/TIGER{vintage}/"
        html = self._fetch_directory(url)

        rows = []
        for match in re.finditer(r'<a href="([A-Z][A-Z0-9_]+/)">', html):
            layer = match.group(1).rstrip("/")
            desc = TIGER_LAYER_DESCRIPTIONS.get(layer, "")
            scope = TIGER_LAYER_SCOPE.get(layer, "unknown")

            if keyword:
                searchable = f"{layer} {desc}".lower()
                if keyword.lower() not in searchable:
                    continue

            rows.append(
                {
                    "layer": layer,
                    "description": desc,
                    "scope": scope,
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("layer").reset_index(drop=True)
        return df

    def _list_cartographic_layers(self, vintage: int, keyword: str | None = None) -> pd.DataFrame:
        url = f"{self.BASE}/GENZ{vintage}/shp/"
        html = self._fetch_directory(url)

        # Cartographic files are named: cb_{year}_{fips}_{layer}_{resolution}.zip
        # Extract unique layer names from filenames.
        layers = set()
        for match in re.finditer(r"cb_\d{4}_\w+_(\w+)_\d+k\.zip", html):
            layers.add(match.group(1))

        rows = []
        for layer in sorted(layers):
            desc = CARTOGRAPHIC_LAYER_DESCRIPTIONS.get(layer, "")

            if keyword:
                searchable = f"{layer} {desc}".lower()
                if keyword.lower() not in searchable:
                    continue

            rows.append(
                {
                    "layer": layer,
                    "description": desc,
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("layer").reset_index(drop=True)
        return df

    # ------------------------------------------------------------------ #
    #  Files
    # ------------------------------------------------------------------ #

    def list_files(
        self,
        vintage: int,
        layer: str,
        source: str = "tiger",
    ) -> pd.DataFrame:
        """
        List the downloadable files for a given vintage + layer.

        Parameters
        ----------
        vintage : int
        layer : str
            e.g. "TRACT", "PRIMARYROADS" (TIGER) or "tract", "county" (cartographic).
        source : str
            "tiger" or "cartographic".

        Returns a DataFrame with columns: filename, state_fips, size, url.
        """
        if source == "tiger":
            return self._list_tiger_files(vintage, layer)
        elif source == "cartographic":
            return self._list_cartographic_files(vintage, layer)
        else:
            raise ValueError(f"Unknown source {source!r}.")

    def _list_tiger_files(self, vintage: int, layer: str) -> pd.DataFrame:
        layer = layer.upper()
        url = f"{self.BASE}/TIGER{vintage}/{layer}/"
        html = self._fetch_directory(url)
        entries = self._parse_directory_links(html)

        rows = []
        for entry in entries:
            name = entry["name"]
            if not name.endswith(".zip"):
                continue

            # Extract state FIPS from filename: tl_{year}_{fips}_{layer}.zip
            fips_match = re.match(r"tl_\d{4}_(\d{2})_", name)
            state_fips = fips_match.group(1) if fips_match else None

            rows.append(
                {
                    "filename": name,
                    "state_fips": state_fips,
                    "size": entry["size"],
                    "url": f"{url}{name}",
                }
            )

        return pd.DataFrame(rows)

    def _list_cartographic_files(self, vintage: int, layer: str) -> pd.DataFrame:
        layer = layer.lower()
        url = f"{self.BASE}/GENZ{vintage}/shp/"
        html = self._fetch_directory(url)
        entries = self._parse_directory_links(html)

        rows = []
        for entry in entries:
            name = entry["name"]
            if not name.endswith(".zip"):
                continue
            if f"_{layer}_" not in name:
                continue

            # Extract state FIPS: cb_{year}_{fips}_{layer}_{res}.zip
            fips_match = re.match(r"cb_\d{4}_(\d{2})_", name)
            state_fips = fips_match.group(1) if fips_match else None

            # Extract resolution
            res_match = re.search(r"_(\d+[km])\.zip", name)
            resolution = res_match.group(1) if res_match else None

            rows.append(
                {
                    "filename": name,
                    "state_fips": state_fips,
                    "resolution": resolution,
                    "size": entry["size"],
                    "url": f"{url}{name}",
                }
            )

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------ #
    #  Download URLs
    # ------------------------------------------------------------------ #

    def get_download_url(
        self,
        vintage: int,
        layer: str,
        source: str = "tiger",
        state_fips: str | None = None,
        resolution: str = "500k",
    ) -> str:
        """
        Build the download URL for a specific file.

        Parameters
        ----------
        vintage : int
        layer : str
        source : str
            "tiger" or "cartographic".
        state_fips : str, optional
            Two-digit FIPS code. If None, returns the national file URL.
        resolution : str
            For cartographic files only. Default "500k".

        Returns the full URL string.
        """
        if source == "tiger":
            layer = layer.upper()
            fips = state_fips or "us"
            filename = f"tl_{vintage}_{fips}_{layer.lower()}.zip"
            return f"{self.BASE}/TIGER{vintage}/{layer}/{filename}"
        elif source == "cartographic":
            layer = layer.lower()
            fips = state_fips or "us"
            filename = f"cb_{vintage}_{fips}_{layer}_{resolution}.zip"
            return f"{self.BASE}/GENZ{vintage}/shp/{filename}"
        else:
            raise ValueError(f"Unknown source {source!r}.")
