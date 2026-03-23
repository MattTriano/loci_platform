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
from pathlib import Path

import geopandas as gpd
import networkx as nx
from loci.collectors.utils import make_temp_dir

log = logging.getLogger(__name__)


class OsmnxClient:
    def __init__(self, cache_dir: str | Path | None = None):
        if cache_dir:
            self.cache_dir = Path(cache_dir)
            self._temp_dir_handle = None
        else:
            self.cache_dir, self._temp_dir_handle = make_temp_dir(prefix="osmnx_cache_")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_bike_network(
        self,
        bbox: tuple[float, float, float, float],
        network_type: str = "bike",
    ) -> nx.MultiDiGraph:
        cache_path = self._cache_path(bbox, network_type)

        if cache_path and cache_path.exists():
            log.info("Loading cached graph from %s", cache_path)
            import osmnx as ox

            return ox.load_graphml(cache_path)

        log.info("Downloading %s network for bbox %s", network_type, bbox)
        import osmnx as ox

        west, south, east, north = bbox
        G = ox.graph_from_bbox(
            bbox=(west, south, east, north),
            network_type=network_type,
            retain_all=True,
        )
        log.info("Downloaded graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())

        if cache_path:
            log.info("Caching graph to %s", cache_path)
            ox.save_graphml(G, cache_path)

        return G

    def get_nodes_gdf(self, G: nx.MultiDiGraph) -> gpd.GeoDataFrame:
        import osmnx as ox

        return ox.graph_to_gdfs(G, nodes=True, edges=False).reset_index()

    def get_edges_gdf(self, G: nx.MultiDiGraph) -> gpd.GeoDataFrame:
        import osmnx as ox

        return ox.graph_to_gdfs(G, nodes=False, edges=True).reset_index()

    def _cache_path(self, bbox, network_type) -> Path | None:
        if not self.cache_dir:
            return None
        key = f"{network_type}_{bbox[0]}_{bbox[1]}_{bbox[2]}_{bbox[3]}"
        h = hashlib.md5(key.encode()).hexdigest()[:12]
        return self.cache_dir / f"osmnx_{network_type}_{h}.graphml"
