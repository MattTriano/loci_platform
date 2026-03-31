"""
Core bike-safety routing logic.

Pure functions that operate on a NetworkX DiGraph and a KDTree index.
No Lambda, S3, or global-state dependencies — easy to test and reuse.

Turn-cost model
---------------
Left turns (turns that cross oncoming traffic) at intersections are
penalized at routing time.  A node counts as an intersection when its
graph degree is >= 3.  The penalty scales with the road classification
of the *target* edge the cyclist is turning onto — crossing a lane of
fast arterial traffic is far riskier than turning left onto a quiet
residential street.

The penalty is additive (meters of "virtual safety cost") so it
composes naturally with the per-edge safety_cost already stored in the
graph.
"""

from __future__ import annotations

import heapq
import math
from collections.abc import Callable

import networkx as nx
from scipy.spatial import KDTree

# =====================================================================
# Left-turn penalty configuration
# =====================================================================
# Minimum unsigned turn angle (degrees) to be considered a real turn
# rather than a gentle curve.  Prevents small bearing changes at
# 3-way junctions from triggering the penalty.
_MIN_TURN_ANGLE_DEG = 45

# Base penalty in "virtual meters" of safety cost, scaled by the road
# classification of the edge being turned onto.
_LEFT_TURN_BASE_PENALTY = 30  # meters

# Multiplier by highway type of the *target* edge (the road the cyclist
# turns onto).  Higher = scarier road to cross traffic on.
_ROAD_CLASS_MULTIPLIER: dict[str, float] = {
    "primary": 2.5,
    "primary_link": 2.0,
    "secondary": 2.0,
    "secondary_link": 1.5,
    "tertiary": 1.5,
    "tertiary_link": 1.2,
    "residential": 0.8,
    "living_street": 0.5,
    "service": 0.5,
    "cycleway": 0.0,  # no oncoming motor traffic
    "path": 0.0,
    "pedestrian": 0.0,
}
_DEFAULT_ROAD_MULTIPLIER = 1.0


# =====================================================================
# KD-tree helpers (unchanged)
# =====================================================================
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


# =====================================================================
# Turn geometry helpers
# =====================================================================
def _bearing(G: nx.DiGraph, from_node: int, to_node: int) -> float:
    """Bearing in radians from from_node to to_node (standard math angle)."""
    f, t = G.nodes[from_node], G.nodes[to_node]
    return math.atan2(t["x"] - f["x"], t["y"] - f["y"])


def _turn_angle_deg(bearing_in: float, bearing_out: float) -> float:
    """Unsigned turn angle in degrees (0 = straight, 180 = U-turn)."""
    diff = abs(math.degrees(bearing_out - bearing_in)) % 360
    return diff if diff <= 180 else 360 - diff


def _is_left_turn(bearing_in: float, bearing_out: float) -> bool:
    """True if the turn from bearing_in to bearing_out is a left turn.

    Uses the cross-product sign of the direction vectors.  In a
    coordinate system where x = longitude, y = latitude (northern
    hemisphere), a negative cross product means turning left.
    """
    dx_in = math.sin(bearing_in)
    dy_in = math.cos(bearing_in)
    dx_out = math.sin(bearing_out)
    dy_out = math.cos(bearing_out)
    cross = dx_in * dy_out - dy_in * dx_out
    return cross < 0


def compute_left_turn_penalty(
    G: nx.DiGraph,
    prev: int,
    curr: int,
    next_node: int,
    next_edge_data: dict,
) -> float:
    """Return the left-turn penalty (in virtual safety-cost meters) for
    the transition prev → curr → next_node.

    Returns 0 if:
      - curr is not an intersection (degree < 3)
      - the turn angle is too small (gentle curve)
      - the turn is a right turn
      - the target road has no oncoming motor traffic
    """
    # Only penalize actual intersections, not road bends
    if G.degree(curr) < 3:
        return 0.0

    bearing_in = _bearing(G, prev, curr)
    bearing_out = _bearing(G, curr, next_node)

    angle = _turn_angle_deg(bearing_in, bearing_out)
    if angle < _MIN_TURN_ANGLE_DEG:
        return 0.0

    if not _is_left_turn(bearing_in, bearing_out):
        return 0.0

    # Scale by road classification of the target edge
    highway = next_edge_data.get("highway", "")
    # highway can be a list-like string from OSM ("['residential', 'tertiary']");
    # extract the first recognizable type.
    if highway and highway.startswith("["):
        highway = highway.strip("[]'\" ").split("'")[0].split(",")[0].strip()

    multiplier = _ROAD_CLASS_MULTIPLIER.get(highway, _DEFAULT_ROAD_MULTIPLIER)
    if multiplier == 0.0:
        return 0.0

    return _LEFT_TURN_BASE_PENALTY * multiplier


# =====================================================================
# Turn-aware A* implementation
# =====================================================================
def _astar_with_turn_costs(
    G: nx.DiGraph,
    source: int,
    target: int,
    heuristic: Callable[[int, int], float],
) -> list[int]:
    """A* shortest path with left-turn penalties at intersections.

    This is a standard A* over (node, predecessor) states so that the
    turn cost — which depends on the incoming direction — is properly
    accounted for.  The state space is only expanded at intersection
    nodes (degree >= 3); non-intersection nodes have a single
    predecessor context, keeping the effective search space close to
    plain A*.

    Parameters
    ----------
    G : nx.DiGraph
        Routing graph with 'safety_cost' edge attribute and 'x'/'y'
        node attributes.
    source, target : int
        Origin and destination node IDs.
    heuristic : callable(u, v) -> float
        Admissible heuristic estimating cost from u to v.

    Returns
    -------
    list[int]
        Ordered list of node IDs from source to target.

    Raises
    ------
    ValueError
        If no path exists.
    """
    # Priority queue entries: (f_score, counter, current_node, prev_node)
    # prev_node is None for the source.
    counter = 0
    open_set: list[tuple[float, int, int, int | None]] = []
    heapq.heappush(open_set, (heuristic(source, target), counter, source, None))

    # Best known g_score for (node, prev_node) states.
    # For non-intersection nodes we collapse prev to None to save memory.
    g_score: dict[tuple[int, int | None], float] = {(source, None): 0.0}

    # Track predecessors for path reconstruction.
    came_from: dict[tuple[int, int | None], tuple[int, int | None]] = {}

    # Track which (node, prev) states have been fully processed.
    closed: set[tuple[int, int | None]] = set()

    while open_set:
        f, _, curr, prev = heapq.heappop(open_set)

        state = (curr, prev)
        if curr == target:
            # Reconstruct path
            path = [curr]
            s = state
            while s in came_from:
                s = came_from[s]
                path.append(s[0])
            path.reverse()
            return path

        if state in closed:
            continue
        closed.add(state)

        curr_g = g_score[state]

        for _, next_node, edge_data in G.edges(curr, data=True):
            edge_cost = edge_data.get("safety_cost", 0.0)

            # Compute turn penalty if we have a predecessor
            turn_penalty = 0.0
            if prev is not None:
                turn_penalty = compute_left_turn_penalty(
                    G,
                    prev,
                    curr,
                    next_node,
                    edge_data,
                )

            tentative_g = curr_g + edge_cost + turn_penalty

            # State key for the next node: track prev only at intersections
            next_prev = curr if G.degree(next_node) >= 3 else None
            next_state = (next_node, next_prev)

            if next_state in closed:
                continue

            if tentative_g < g_score.get(next_state, math.inf):
                g_score[next_state] = tentative_g
                came_from[next_state] = state
                f_score = tentative_g + heuristic(next_node, target)
                counter += 1
                heapq.heappush(
                    open_set,
                    (f_score, counter, next_node, curr if G.degree(next_node) >= 3 else None),
                )

    raise ValueError(f"No route found between {source} and {target}")


# =====================================================================
# Public routing API
# =====================================================================
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

    Uses A* with safety_cost edge weights and left-turn penalties at
    intersections.

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

    path = _astar_with_turn_costs(G, origin_node, dest_node, heuristic)

    total_cost = 0.0
    total_length_m = 0.0
    coordinates = []

    prev_node = None
    for u, v in zip(path[:-1], path[1:], strict=True):
        edge_data = G[u][v]
        edge_cost = edge_data.get("safety_cost", 0.0)
        total_length_m += edge_data.get("length_m", 0.0)

        # Include turn penalty in the reported total_cost
        turn_penalty = 0.0
        if prev_node is not None:
            turn_penalty = compute_left_turn_penalty(
                G,
                prev_node,
                u,
                v,
                edge_data,
            )
        total_cost += edge_cost + turn_penalty
        prev_node = u

        edge_coords = edge_data.get("geometry_coords")
        if edge_coords:
            u_data = G.nodes[u]
            first = edge_coords[0]
            last = edge_coords[-1]
            dist_to_first = (first[0] - u_data["x"]) ** 2 + (first[1] - u_data["y"]) ** 2
            dist_to_last = (last[0] - u_data["x"]) ** 2 + (last[1] - u_data["y"]) ** 2
            oriented = edge_coords if dist_to_first <= dist_to_last else list(reversed(edge_coords))

            if coordinates:
                oriented = oriented[1:]
            coordinates.extend(oriented)
        else:
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
