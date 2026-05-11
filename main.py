"""Profile find_route on a real graph file.

Usage:
    # Download the graph file from S3 first:
    aws s3 cp s3://loci-infra-dev-chicago-bike-map-routing-graph/graph/routing_graph.pkl.gz /tmp/

    # Then run:
    python profile_routing.py /tmp/routing_graph.pkl.gz

The script routes between a few origin/destination pairs of varying
distance and prints a cProfile summary for each. Compare two builds of
the graph file by running this script twice (renaming each .prof file
between runs) to see what got faster or slower.
"""

import cProfile
import gzip
import pickle
import pstats
import sys
import time
from pathlib import Path

# Make the routing module importable. Adjust if your layout differs.
sys.path.insert(0, str(Path(__file__).parent / "platform/airflow/dags/loci"))
from routing import build_kdtree, find_route  # noqa: E402

# A handful of routes spanning short / medium / long distances.
# Pick coordinates that are realistic for your city. Lat/lon pairs.
TEST_ROUTES = [
    # (label, origin_lat, origin_lon, dest_lat, dest_lon)
    ("short_2mi", 41.8781, -87.6298, 41.9050, -87.6350),  # Loop -> near North side
    ("medium_8mi", 41.8500, -87.6500, 41.9700, -87.7000),  # SW -> NW
    ("long_20mi", 41.6800, -87.6500, 41.9900, -87.6700),  # Far south -> Far north
    ("pre_change_long_route", 41.9471, -87.6562, 41.7978, -87.6648),
    ("post_change_long_route", 41.9637, -87.6559, 41.7655, -87.6926),
]


def load_graph(path: str):
    print(f"Loading graph from {path}...")
    t0 = time.perf_counter()
    with open(path, "rb") as f:
        compressed = f.read()
    G = pickle.loads(gzip.decompress(compressed))
    elapsed = time.perf_counter() - t0
    print(f"  loaded {G.number_of_nodes()} nodes, {G.number_of_edges()} edges in {elapsed:.2f}s")
    print(f"  heuristic_floor = {G.graph.get('heuristic_floor', 'NOT SET')}")
    return G


def time_route(G, kdtree, node_ids, label, o_lat, o_lon, d_lat, d_lon, runs=3):
    """Run a route a few times and report the median wall time."""
    # Warm up first (ensures any lazy attribute caching has happened).
    find_route(G, kdtree, node_ids, o_lat, o_lon, d_lat, d_lon)

    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        result = find_route(G, kdtree, node_ids, o_lat, o_lon, d_lat, d_lon)
        times.append(time.perf_counter() - t0)
    times.sort()
    median = times[len(times) // 2]
    print(
        f"  {label}: median {median * 1000:.0f}ms across {runs} runs "
        f"({result['total_length_m']:.0f}m route, {len(result['segments'])} segments)"
    )
    return median


def profile_route(G, kdtree, node_ids, label, o_lat, o_lon, d_lat, d_lon):
    """Run a single route under cProfile and dump a .prof file."""
    # Warm up
    find_route(G, kdtree, node_ids, o_lat, o_lon, d_lat, d_lon)

    profile_path = f"profile_{label}.prof"
    profiler = cProfile.Profile()
    profiler.enable()
    find_route(G, kdtree, node_ids, o_lat, o_lon, d_lat, d_lon)
    profiler.disable()

    profiler.dump_stats(profile_path)
    print(f"\nProfile for {label} (saved to {profile_path}):")
    stats = pstats.Stats(profiler).sort_stats("cumulative")
    stats.print_stats(20)  # Top 20 functions by cumulative time


def main():
    if len(sys.argv) != 2:
        print("usage: python profile_routing.py <path-to-graph.pkl.gz>")
        sys.exit(1)

    G = load_graph(sys.argv[1])
    print("\nBuilding KD-tree...")
    t0 = time.perf_counter()
    kdtree, node_ids = build_kdtree(G)
    print(f"  built in {time.perf_counter() - t0:.2f}s\n")

    print("=" * 60)
    print("Wall-time medians (3 runs each):")
    print("=" * 60)
    for route in TEST_ROUTES:
        time_route(G, kdtree, node_ids, *route)

    print()
    print("=" * 60)
    print("cProfile of long_20mi route:")
    print("=" * 60)
    profile_route(G, kdtree, node_ids, *TEST_ROUTES[-1])


if __name__ == "__main__":
    main()
