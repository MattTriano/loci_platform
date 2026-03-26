"""
Core bike-safety routing logic.

Pure functions that operate on a NetworkX DiGraph and a KDTree index.
No Lambda, S3, or global-state dependencies — easy to test and reuse.
"""

import math

import networkx as nx
from scipy.spatial import KDTree


def build_kdtree(G: nx.DiGraph) -> tuple[KDTree, list]:
    """Build a KD-tree over node coordinates for fast nearest-node lookup.

    Returns (kdtree, node_ids) where node_ids[i] corresponds to the
    i-th point in the KD-tree.
    """
    node_ids = list(G.nodes())
    coords = [(G.nodes[n]["y"], G.nodes[n]["x"]) for n in node_ids]
    return KDTree(coords), node_ids


def nearest_node(kdtree: KDTree, node_ids: list, lat: float, lon: float) -> int:
    """Return the osmid of the nearest graph node to (lat, lon)."""
    _, idx = kdtree.query([lat, lon])
    return node_ids[idx]


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in meters between two lat/lon points."""
    r = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def find_route(
    G: nx.DiGraph,
    kdtree: KDTree,
    node_ids: list,
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
) -> dict:
    """Find the safest route between two coordinates.

    Returns a dict with total_cost, total_length_m, nodes, and coordinates.
    """
    origin_node = nearest_node(kdtree, node_ids, origin_lat, origin_lon)
    dest_node = nearest_node(kdtree, node_ids, dest_lat, dest_lon)

    if origin_node == dest_node:
        node_data = G.nodes[origin_node]
        return {
            "total_cost": 0.0,
            "total_length_m": 0.0,
            "nodes": [origin_node],
            "coordinates": [[node_data["x"], node_data["y"]]],
        }

    def heuristic(u, v):
        u_data = G.nodes[u]
        v_data = G.nodes[v]
        return haversine_m(u_data["y"], u_data["x"], v_data["y"], v_data["x"])

    try:
        path = nx.astar_path(
            G,
            origin_node,
            dest_node,
            heuristic=heuristic,
            weight="safety_cost",
        )
    except nx.NetworkXNoPath:
        raise ValueError(f"No route found between {origin_node} and {dest_node}")

    total_cost = 0.0
    total_length_m = 0.0
    coordinates = []

    for u, v in zip(path[:-1], path[1:], strict=True):
        edge_data = G[u][v]
        total_cost += edge_data.get("safety_cost", 0.0)
        total_length_m += edge_data.get("length_m", 0.0)

        edge_coords = edge_data.get("geometry_coords")
        if edge_coords:
            # The stored geometry may run opposite to the traversal direction.
            # Check if the first point is closer to node u or node v.
            u_data = G.nodes[u]
            first = edge_coords[0]
            last = edge_coords[-1]
            dist_to_first = (first[0] - u_data["x"]) ** 2 + (first[1] - u_data["y"]) ** 2
            dist_to_last = (last[0] - u_data["x"]) ** 2 + (last[1] - u_data["y"]) ** 2
            oriented = edge_coords if dist_to_first <= dist_to_last else list(reversed(edge_coords))

            # Skip the first point of each edge after the first to avoid duplicates
            # at edge junctions (the last point of one edge = the first of the next).
            if coordinates:
                oriented = oriented[1:]
            coordinates.extend(oriented)
        else:
            # Fallback: straight line between nodes (for edges without geometry)
            if coordinates:
                coordinates.append([G.nodes[v]["x"], G.nodes[v]["y"]])
            else:
                coordinates.append([G.nodes[u]["x"], G.nodes[u]["y"]])
                coordinates.append([G.nodes[v]["x"], G.nodes[v]["y"]])

    return {
        "total_cost": round(total_cost, 4),
        "total_length_m": round(total_length_m, 1),
        "nodes": path,
        "coordinates": coordinates,
    }


def get_route_street_names(G: nx.DiGraph, nodes: list) -> set[str]:
    """Extract the set of street names used by a route.

    Useful for asserting that a route does or doesn't use specific streets.
    Skips edges with no name attribute.
    """
    names = set()
    for u, v in zip(nodes[:-1], nodes[1:], strict=True):
        name = G[u][v].get("name")
        if name:
            names.add(name)
    return names
