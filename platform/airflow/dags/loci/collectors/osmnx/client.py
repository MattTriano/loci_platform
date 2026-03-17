"""
OsmnxClient — downloads bike network graphs via OSMnx.

Wraps osmnx.graph_from_bbox with caching to avoid repeated Overpass
API calls. The cache is a local GraphML file keyed by bbox and
network type.

Usage:
    from loci.collectors.osmnx.client import OsmnxClient

    client = OsmnxClient(cache_dir="/tmp/osmnx_cache")
    G = client.get_bike_network(bbox=(-87.97, 41.62, -87.5, 42.05))
"""

import hashlib
import logging
from pathlib import Path

import networkx as nx
from loci.collectors.utils import make_temp_dir

log = logging.getLogger(__name__)


class OsmnxClient:
    """HTTP-free client that downloads OSM network graphs via OSMnx.

    Parameters
    ----------
    cache_dir : str or Path or None
        Directory to cache downloaded graphs as GraphML files.
        If None, no caching is performed.
    """

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
            The street network graph with node/edge attributes from OSM.
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
