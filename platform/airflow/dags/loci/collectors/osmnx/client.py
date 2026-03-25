"""
OsmnxClient — downloads bike network graphs via OSMnx.

Wraps osmnx.graph_from_bbox with caching to avoid repeated Overpass
API calls. The cache is a local GraphML file keyed by bbox and
network type.

By default, fetches the standard OSMnx bike network *plus* footways
tagged bicycle=yes/designated/permissive (crossings, shared paths, etc.)
that the default bike filter excludes. After merging, nearby nodes are
contracted (respecting vertical separation) to restore topological
connectivity. This produces a more connected cycling graph.

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
import numpy as np
from loci.collectors.utils import make_temp_dir
from scipy.spatial import cKDTree

log = logging.getLogger(__name__)


class OsmnxClient:
    # Footways/sidewalks that are explicitly tagged as bikeable.
    # These are excluded by osmnx's default bike filter but are
    # critical for network connectivity (crossings, park connectors, etc.)
    _BIKEABLE_FOOTWAY_FILTER = (
        '["highway"~"footway|sidewalk"]'
        '["bicycle"~"yes|designated|permissive"]'
        '["area"!~"yes"]'
        '["service"!~"private"]'
    )

    def __init__(self, cache_dir: str | Path | None = None):
        if cache_dir:
            self.cache_dir = Path(cache_dir)
            self._temp_dir_handle = None
        else:
            self.cache_dir, self._temp_dir_handle = make_temp_dir(prefix="osmnx_cache_")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def get_bike_network(
        self,
        bbox: tuple[float, float, float, float],
        network_type: str = "bike",
        include_bikeable_footways: bool = True,
        merge_threshold_meters: float | None = 15,
    ) -> nx.MultiDiGraph:
        """Fetch a bike network graph, optionally including bikeable footways.

        Parameters
        ----------
        bbox : tuple
            (west, south, east, north) in decimal degrees.
        network_type : str
            OSMnx network type for the base graph.
        include_bikeable_footways : bool
            If True (default), also fetch footways tagged bicycle=yes|designated|permissive
            and merge them into the base bike graph. This fills in crossings and
            short connectors that the default bike filter drops.
        merge_threshold_meters : float or None
            After merging supplemental graphs, contract nodes that are within
            this distance of each other (respecting vertical separation from
            bridges/tunnels). This connects intersections that share the same
            physical location but have different OSM node IDs across the two
            graphs. Set to None to disable. Default 15m.

        Returns
        -------
        nx.MultiDiGraph
        """
        cache_key = (
            f"{network_type}_footways={include_bikeable_footways}_merge={merge_threshold_meters}"
        )
        cache_path = self._cache_path(bbox, cache_key)

        if cache_path and cache_path.exists():
            log.info("Loading cached graph from %s", cache_path)
            import osmnx as ox

            return ox.load_graphml(cache_path)

        # --- Base bike network ---
        G = self._fetch_graph(bbox, network_type=network_type)
        log.info(
            "Base bike graph: %d nodes, %d edges",
            G.number_of_nodes(),
            G.number_of_edges(),
        )

        # --- Bikeable footways ---
        footway_nodes: set = set()
        if include_bikeable_footways:
            try:
                G_footways = self._fetch_graph(
                    bbox,
                    network_type=None,
                    custom_filter=self._BIKEABLE_FOOTWAY_FILTER,
                )
                log.info(
                    "Bikeable footways graph: %d nodes, %d edges",
                    G_footways.number_of_nodes(),
                    G_footways.number_of_edges(),
                )
                # Track which nodes are footway-only (not already in the base graph)
                footway_nodes = set(G_footways.nodes) - set(G.nodes)
                G = self._merge_graphs(G, G_footways)
                log.info(
                    "Merged graph: %d nodes, %d edges (%d footway-only nodes)",
                    G.number_of_nodes(),
                    G.number_of_edges(),
                    len(footway_nodes),
                )
            except Exception as e:
                if "Found no graph nodes" in str(e) or "EmptyOverpassResponse" in str(e):
                    log.info("No bikeable footways found in bbox %s, skipping merge", bbox)
                else:
                    raise

        # --- Connect nearby nodes across the merged graphs ---
        if merge_threshold_meters is not None and footway_nodes:
            G = self._connect_near_nodes(
                G,
                threshold_meters=merge_threshold_meters,
                cross_graph_nodes=footway_nodes,
            )

        if cache_path:
            import osmnx as ox

            log.info("Caching graph to %s", cache_path)
            ox.save_graphml(G, cache_path)

        return G

    def get_nodes_gdf(self, G: nx.MultiDiGraph) -> gpd.GeoDataFrame:
        import osmnx as ox

        return ox.graph_to_gdfs(G, nodes=True, edges=False).reset_index()

    def get_edges_gdf(self, G: nx.MultiDiGraph) -> gpd.GeoDataFrame:
        import osmnx as ox

        return ox.graph_to_gdfs(G, nodes=False, edges=True).reset_index()

    # ------------------------------------------------------------------ #
    #  Graph fetching & merging
    # ------------------------------------------------------------------ #

    def _fetch_graph(
        self,
        bbox: tuple[float, float, float, float],
        network_type: str | None = "bike",
        custom_filter: str | None = None,
    ) -> nx.MultiDiGraph:
        """Download a single graph from Overpass via OSMnx."""
        import osmnx as ox

        west, south, east, north = bbox
        kwargs = dict(
            bbox=(west, south, east, north),
            retain_all=True,
        )
        if custom_filter:
            kwargs["custom_filter"] = custom_filter
            kwargs["network_type"] = None
        else:
            kwargs["network_type"] = network_type

        return ox.graph_from_bbox(**kwargs)

    @staticmethod
    def _merge_graphs(
        G_base: nx.MultiDiGraph,
        G_extra: nx.MultiDiGraph,
    ) -> nx.MultiDiGraph:
        """Merge G_extra into G_base, preserving attributes from both.

        Uses nx.compose which keeps all nodes and edges from both graphs.
        Where both graphs have the same node, G_extra's attributes take
        lower priority (G_base wins). Edge duplicates are kept since this
        is a MultiDiGraph.
        """
        # nx.compose(H, G) — for shared nodes/edges, G's attributes win.
        # We want G_base to win, so it goes second.
        G_merged = nx.compose(G_extra, G_base)

        # Preserve the CRS and graph-level attributes from the base graph
        G_merged.graph.update(G_base.graph)

        return G_merged

    # ------------------------------------------------------------------ #
    #  Near-node contraction
    # ------------------------------------------------------------------ #

    @staticmethod
    def _connect_near_nodes(
        G: nx.MultiDiGraph,
        threshold_meters: float = 15,
        cross_graph_nodes: set | None = None,
    ) -> nx.MultiDiGraph:
        """Merge nodes that are within `threshold_meters` of each other.

        After composing two graphs, they may share the same real-world
        intersection but have separate node IDs. This finds those pairs
        and contracts them, skipping pairs that:
        - are already connected by an edge
        - are both from the same source graph (if cross_graph_nodes is provided)
        - are on different vertical layers (bridges, tunnels, overpasses)

        Parameters
        ----------
        G : nx.MultiDiGraph
            An OSMnx street network graph.
        threshold_meters : float
            Maximum distance in meters between nodes to consider merging.
        cross_graph_nodes : set or None
            Node IDs that came from the supplemental graph (e.g. footways).
            If provided, only pairs where exactly one node is in this set
            will be merged. The base-graph node is always kept.

        Returns
        -------
        nx.MultiDiGraph
            A copy of the graph with near-duplicate nodes contracted.
        """
        nodes = list(G.nodes(data=True))
        node_ids = [n for n, _ in nodes]

        if len(node_ids) < 2:
            return G

        # Build a KD-tree in approximate meter coordinates
        lats = np.array([d["y"] for _, d in nodes])
        lons = np.array([d["x"] for _, d in nodes])

        lat_center = lats.mean()
        meters_per_deg_lat = 111_320
        meters_per_deg_lon = 111_320 * np.cos(np.radians(lat_center))

        coords = np.column_stack(
            [
                lats * meters_per_deg_lat,
                lons * meters_per_deg_lon,
            ]
        )

        tree = cKDTree(coords)
        pairs = tree.query_pairs(r=threshold_meters)

        # Pre-compute layer and grade-separation info per node
        node_layers = {n: OsmnxClient._get_node_layers(G, n) for n in node_ids}
        node_grade_sep = {n: OsmnxClient._is_grade_separated(G, n) for n in node_ids}

        merge_map = {}
        skipped_layer = 0
        skipped_grade = 0
        skipped_same_graph = 0

        for i, j in pairs:
            id_i, id_j = node_ids[i], node_ids[j]

            # Already connected — no need to merge
            if G.has_edge(id_i, id_j) or G.has_edge(id_j, id_i):
                continue

            # Only merge cross-graph pairs: exactly one node from each graph
            if cross_graph_nodes is not None:
                i_is_extra = id_i in cross_graph_nodes
                j_is_extra = id_j in cross_graph_nodes
                if i_is_extra == j_is_extra:
                    # Both from the same graph — skip
                    skipped_same_graph += 1
                    continue

            # Skip if nodes have no overlapping layers
            if not node_layers[id_i].intersection(node_layers[id_j]):
                skipped_layer += 1
                continue

            # Skip if one node is grade-separated and the other isn't
            if node_grade_sep[id_i] != node_grade_sep[id_j]:
                skipped_grade += 1
                continue

            # Keep the base-graph node, drop the supplemental one
            if cross_graph_nodes is not None:
                if id_i in cross_graph_nodes:
                    keep, drop = id_j, id_i
                else:
                    keep, drop = id_i, id_j
            else:
                keep, drop = (id_i, id_j) if id_i < id_j else (id_j, id_i)

            merge_map[drop] = keep

        # Resolve transitive chains: if A→B and B→C, make A→C
        merge_map = {k: OsmnxClient._resolve_merge_chain(merge_map, k) for k in merge_map}

        log.info(
            "connect_near_nodes: %d pairs to merge, "
            "skipped %d (same graph), %d (layer mismatch), "
            "%d (grade separation mismatch)",
            len(merge_map),
            skipped_same_graph,
            skipped_layer,
            skipped_grade,
        )

        if not merge_map:
            return G

        return OsmnxClient._apply_merges(G, merge_map)

    # ------------------------------------------------------------------ #
    #  Vertical separation helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _get_node_layers(G: nx.MultiDiGraph, node) -> set[str]:
        """Get the set of layer values from all edges touching this node.

        Defaults to "0" (ground level) when no layer tag is present,
        which is the OSM convention.
        """
        layers = set()
        for _, _, data in G.edges(node, data=True):
            layers.add(str(data.get("layer", "0")))
        for _, _, data in G.in_edges(node, data=True):
            layers.add(str(data.get("layer", "0")))
        # Isolated node with no edges — treat as ground level
        if not layers:
            layers.add("0")
        return layers

    @staticmethod
    def _is_grade_separated(G: nx.MultiDiGraph, node) -> bool:
        """Check if ANY edge at this node is a bridge, viaduct, or tunnel.

        A node is grade-separated if it sits on infrastructure that is
        vertically offset from ground level.
        """
        for _, _, data in G.edges(node, data=True):
            if OsmnxClient._edge_is_grade_separated(data):
                return True
        for _, _, data in G.in_edges(node, data=True):
            if OsmnxClient._edge_is_grade_separated(data):
                return True
        return False

    @staticmethod
    def _edge_is_grade_separated(data: dict) -> bool:
        """Check if a single edge's tags indicate grade separation."""
        bridge = data.get("bridge", "")
        if isinstance(bridge, list):
            bridge = bridge[0] if bridge else ""
        if str(bridge).lower() in ("yes", "viaduct", "movable", "cantilever"):
            return True

        tunnel = data.get("tunnel", "")
        if isinstance(tunnel, list):
            tunnel = tunnel[0] if tunnel else ""
        if str(tunnel).lower() in ("yes", "building_passage", "covered"):
            return True

        return False

    # ------------------------------------------------------------------ #
    #  Merge execution helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _resolve_merge_chain(merge_map: dict, node) -> any:
        """Follow merge chains to their final target."""
        visited = set()
        while node in merge_map and node not in visited:
            visited.add(node)
            node = merge_map[node]
        return node

    @staticmethod
    def _apply_merges(
        G: nx.MultiDiGraph,
        merge_map: dict,
    ) -> nx.MultiDiGraph:
        """Rewire edges and remove merged nodes.

        For each dropped node, all its edges are rewired to point to/from
        the kept node instead, then the dropped node is removed.
        """
        G_new = G.copy()

        for drop, keep in merge_map.items():
            if drop not in G_new:
                continue

            # Rewire outgoing edges
            for _, v, key, data in list(G_new.edges(drop, keys=True, data=True)):
                new_v = merge_map.get(v, v)
                if keep != new_v:  # avoid self-loops
                    G_new.add_edge(keep, new_v, key=key, **data)

            # Rewire incoming edges
            for u, _, key, data in list(G_new.in_edges(drop, keys=True, data=True)):
                new_u = merge_map.get(u, u)
                if new_u != keep:  # avoid self-loops
                    G_new.add_edge(new_u, keep, key=key, **data)

            G_new.remove_node(drop)

        log.info(
            "After merging: %d nodes, %d edges",
            G_new.number_of_nodes(),
            G_new.number_of_edges(),
        )

        return G_new

    # ------------------------------------------------------------------ #
    #  Caching
    # ------------------------------------------------------------------ #

    def _cache_path(self, bbox, cache_key: str) -> Path | None:
        if not self.cache_dir:
            return None
        raw = f"{cache_key}_{bbox[0]}_{bbox[1]}_{bbox[2]}_{bbox[3]}"
        h = hashlib.md5(raw.encode()).hexdigest()[:12]
        # Sanitize cache_key for filename
        safe_key = cache_key.replace("=", "_").replace(" ", "_")
        return self.cache_dir / f"osmnx_{safe_key}_{h}.graphml"
