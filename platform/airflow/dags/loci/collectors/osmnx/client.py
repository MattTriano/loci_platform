"""
OsmnxClient — downloads bike network graphs via OSMnx.

Wraps osmnx.graph_from_bbox with caching to avoid repeated Overpass
API calls. The cache is a local GraphML file keyed by bbox and
network type.

Usage:
    from loci.collectors.osmnx.client import OsmnxClient

    client = OsmnxClient(cache_dir="/tmp/osmnx_cache")

    # Iterate over tiles, keeping only one graph in memory at a time:
    for tile_id, nodes_gdf, edges_gdf in client.iter_bike_network_tiles(
        bbox=(-87.97, 41.62, -87.5, 42.05),
        max_tile_degrees=0.3,
    ):
        ...

    # Or fetch a single graph and convert yourself:
    G = client.get_bike_network(bbox=(-87.97, 41.62, -87.5, 42.05))
    nodes_gdf = client.get_nodes_gdf(G)
    edges_gdf = client.get_edges_gdf(G)
"""

import hashlib
import logging
import math
from collections.abc import Iterator
from pathlib import Path

import geopandas as gpd
import networkx as nx
from loci.collectors.utils import make_temp_dir

log = logging.getLogger(__name__)


class OsmnxClient:
    """HTTP-free client that downloads OSM network graphs via OSMnx.

    Parameters
    ----------
    cache_dir : str or Path or None
        Directory to cache downloaded graphs as GraphML files.
        If None, a temporary directory is used.
    """

    def __init__(self, cache_dir: str | Path | None = None):
        if cache_dir:
            self.cache_dir = Path(cache_dir)
            self._temp_dir_handle = None
        else:
            self.cache_dir, self._temp_dir_handle = make_temp_dir(prefix="osmnx_cache_")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Graph download
    # ------------------------------------------------------------------

    def get_bike_network(
        self,
        bbox: tuple[float, float, float, float],
        network_type: str = "bike",
    ) -> nx.MultiDiGraph:
        """Download or load from cache a bike network graph.

        Parameters
        ----------
        bbox : tuple
            (west, south, east, north) in EPSG:4326.
        network_type : str
            OSMnx network type. Default "bike".

        Returns
        -------
        networkx.MultiDiGraph
        """
        cache_path = self._cache_path(bbox, network_type)

        if cache_path and cache_path.exists():
            log.info("Loading cached graph from %s", cache_path)
            import osmnx as ox

            return ox.load_graphml(cache_path)

        log.info(
            "Downloading %s network for bbox %s from Overpass API",
            network_type,
            bbox,
        )
        import osmnx as ox

        west, south, east, north = bbox
        G = ox.graph_from_bbox(
            bbox=(west, south, east, north),
            network_type=network_type,
            retain_all=True,
        )

        log.info(
            "Downloaded graph: %d nodes, %d edges",
            G.number_of_nodes(),
            G.number_of_edges(),
        )

        if cache_path:
            log.info("Caching graph to %s", cache_path)
            ox.save_graphml(G, cache_path)

        return G

    # ------------------------------------------------------------------
    # GeoDataFrame conversion
    # ------------------------------------------------------------------

    def get_nodes_gdf(self, G: nx.MultiDiGraph) -> gpd.GeoDataFrame:
        """Extract nodes from a graph as a GeoDataFrame.

        Parameters
        ----------
        G : networkx.MultiDiGraph

        Returns
        -------
        geopandas.GeoDataFrame with osmid as a column (reset from index).
        """
        import osmnx as ox

        gdf = ox.graph_to_gdfs(G, nodes=True, edges=False)
        return gdf.reset_index()

    def get_edges_gdf(self, G: nx.MultiDiGraph) -> gpd.GeoDataFrame:
        """Extract edges from a graph as a GeoDataFrame.

        Parameters
        ----------
        G : networkx.MultiDiGraph

        Returns
        -------
        geopandas.GeoDataFrame with u, v, key as columns (reset from index).
        """
        import osmnx as ox

        gdf = ox.graph_to_gdfs(G, nodes=False, edges=True)
        return gdf.reset_index()

    # ------------------------------------------------------------------
    # Tiled iteration
    # ------------------------------------------------------------------

    def iter_bike_network_tiles(
        self,
        bbox: tuple[float, float, float, float],
        max_tile_degrees: float = 0.3,
        network_type: str = "bike",
    ) -> Iterator[tuple[str, gpd.GeoDataFrame, gpd.GeoDataFrame]]:
        """Iterate over tiles of a bbox, yielding one graph at a time.

        Downloads (or loads from cache) one tile graph, converts it to
        GeoDataFrames, then deletes the graph before moving to the next
        tile. This keeps peak memory bounded to a single tile.

        Parameters
        ----------
        bbox : tuple
            (west, south, east, north) in EPSG:4326.
        max_tile_degrees : float
            Maximum tile size in degrees. Boundaries are snapped to a
            global grid so they are consistent across runs.
        network_type : str
            OSMnx network type. Default "bike".

        Yields
        ------
        (tile_id, nodes_gdf, edges_gdf)
        """
        tiles = self._tile_bbox(bbox, max_tile_degrees)
        log.info("Bbox %s split into %d tiles", bbox, len(tiles))

        for tile in tiles:
            tile_id = self._make_tile_id(tile)
            log.info("Processing tile %s", tile_id)

            try:
                G = self.get_bike_network(bbox=tile, network_type=network_type)
            except Exception as e:
                if "No data elements in server response" in str(e):
                    log.info(
                        "Skipping tile %s — no data returned (e.g. water, unpopulated area)",
                        tile_id,
                    )
                    continue
                raise

            nodes_gdf = self.get_nodes_gdf(G)
            edges_gdf = self.get_edges_gdf(G)
            del G

            yield tile_id, nodes_gdf, edges_gdf

    # ------------------------------------------------------------------
    # Tiling helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _tile_bbox(
        bbox: tuple[float, float, float, float],
        max_tile_degrees: float,
    ) -> list[tuple[float, float, float, float]]:
        """Split a bbox into a grid of tiles snapped to a global grid.

        Tile boundaries are multiples of max_tile_degrees, so the same
        geographic area always produces identical tiles regardless of the
        exact bbox requested.

        Parameters
        ----------
        bbox : tuple
            (west, south, east, north) in EPSG:4326.
        max_tile_degrees : float
            Grid cell size in degrees.

        Returns
        -------
        list of (west, south, east, north) tuples.
        """
        west, south, east, north = bbox

        # Snap the start of the grid to the nearest multiple of max_tile_degrees
        # that is <= the bbox edge
        grid_west = math.floor(west / max_tile_degrees) * max_tile_degrees
        grid_south = math.floor(south / max_tile_degrees) * max_tile_degrees

        tiles = []
        lat = grid_south
        while lat < north:
            lon = grid_west
            while lon < east:
                tile = (
                    round(lon, 10),
                    round(lat, 10),
                    round(min(lon + max_tile_degrees, east), 10),
                    round(min(lat + max_tile_degrees, north), 10),
                )
                tiles.append(tile)
                lon += max_tile_degrees
            lat += max_tile_degrees

        return tiles

    @staticmethod
    def _make_tile_id(tile_bbox: tuple[float, float, float, float]) -> str:
        """Build a stable string identifier for a tile from its corners.

        Format: "osmnx_(W,S)-(E,N)" with coordinates rounded to 6 decimal
        places, e.g. "osmnx_(-87.900000,41.600000)-(-87.600000,41.900000)".
        """
        west, south, east, north = tile_bbox
        return f"osmnx_({west:.6f},{south:.6f})-({east:.6f},{north:.6f})"

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _cache_path(
        self,
        bbox: tuple[float, float, float, float],
        network_type: str,
    ) -> Path | None:
        """Build a deterministic cache file path from bbox + network type."""
        if not self.cache_dir:
            return None
        key = f"{network_type}_{bbox[0]}_{bbox[1]}_{bbox[2]}_{bbox[3]}"
        h = hashlib.md5(key.encode()).hexdigest()[:12]
        return self.cache_dir / f"osmnx_{network_type}_{h}.graphml"
