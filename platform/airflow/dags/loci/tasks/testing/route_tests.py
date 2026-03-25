"""
Route quality tests for the bike safety routing graph.

Loads a real graph file and checks that known routes use (or avoid)
specific streets. These encode local knowledge about which corridors
are safe and which are dangerous for cyclists.

Usage from an Airflow DAG:

    from route_tests import run_route_tests

    test_task = PythonOperator(
        task_id="test_route_quality",
        python_callable=run_route_tests,
        op_kwargs={"graph_path": "/tmp/routing_graph.pkl.gz"},
    )

Each test case is a dict describing a route and what to check about it.
This keeps the test definitions readable and easy to extend — when you
notice a bad route in practice, add a case here to prevent regressions.
"""

import gzip
import logging
import pickle
from dataclasses import dataclass, field
from pathlib import Path

import networkx as nx
from loci.routing import build_kdtree, find_route, get_route_street_names

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Test case definition
# ---------------------------------------------------------------------------


@dataclass
class RouteTestCase:
    """A single route quality assertion.

    Attributes:
        name: Human-readable description of what this test checks.
        origin: (lat, lon) tuple.
        destination: (lat, lon) tuple.
        must_use: Street names the route MUST include (any partial match).
        must_avoid: Street names the route must NOT include (any partial match).
        max_cost: Optional upper bound on total_cost (catches cost explosions).
    """

    name: str
    origin: tuple[float, float]
    destination: tuple[float, float]
    must_use: list[str] = field(default_factory=list)
    must_avoid: list[str] = field(default_factory=list)
    max_cost: float | None = None


# ---------------------------------------------------------------------------
# Test cases — add new ones here as you discover bad routes
# ---------------------------------------------------------------------------

TEST_CASES = [
    # -- Lakefront Trail should be preferred over Lake Shore Drive --
    RouteTestCase(
        name="Uptown to Museum Campus should use Lakefront Trail, not LSD",
        origin=(41.9660, -87.6465),  # Montrose Harbor area
        destination=(41.8665, -87.6070),  # Museum Campus / Shedd area
        must_use=["Lakefront Trail"],
        must_avoid=["Lake Shore Drive", "DuSable Lake Shore Drive"],
    ),
    RouteTestCase(
        name="Edgewater to Hyde Park along lake should use Lakefront Trail",
        origin=(41.9835, -87.6500),  # Edgewater, near the lake
        destination=(41.7945, -87.5805),  # Hyde Park, near the lake
        must_use=["Lakefront Trail"],
        must_avoid=["Lake Shore Drive", "DuSable Lake Shore Drive"],
    ),
    # -- Bloomingdale Trail (the 606) --
    RouteTestCase(
        name="Humboldt Park to Bucktown along 606 corridor should use Bloomingdale Trail",
        origin=(41.9155, -87.7198),  # west end of 606
        destination=(41.9140, -87.6680),  # east end of 606
        must_use=["Bloomingdale Trail"],
    ),
    # -- General: never route onto high-speed arterials --
    RouteTestCase(
        name="Wicker Park to Logan Square should avoid Western Ave",
        origin=(41.9085, -87.6796),  # Wicker Park
        destination=(41.9295, -87.7080),  # Logan Square monument
        must_avoid=["Western Avenue", "North Western Avenue"],
    ),
]


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------


def _street_name_matches(route_names: set[str], pattern: str) -> bool:
    """Check if any street name in the route contains the pattern (case-insensitive)."""
    pattern_lower = pattern.lower()
    return any(pattern_lower in name.lower() for name in route_names)


@dataclass
class TestResult:
    case_name: str
    passed: bool
    message: str


def run_single_test(G: nx.DiGraph, kdtree, node_ids, case: RouteTestCase) -> TestResult:
    """Run a single route test case. Returns a TestResult."""
    try:
        result = find_route(
            G,
            kdtree,
            node_ids,
            case.origin[0],
            case.origin[1],
            case.destination[0],
            case.destination[1],
        )
    except ValueError as e:
        return TestResult(case.name, passed=False, message=f"No route found: {e}")

    street_names = get_route_street_names(G, result["nodes"])
    failures = []

    for required in case.must_use:
        if not _street_name_matches(street_names, required):
            failures.append(f"Expected route to use '{required}' but it didn't")

    for forbidden in case.must_avoid:
        if _street_name_matches(street_names, forbidden):
            failures.append(f"Route should avoid '{forbidden}' but it was used")

    if case.max_cost is not None and result["total_cost"] > case.max_cost:
        failures.append(f"Cost {result['total_cost']:.1f} exceeds max {case.max_cost:.1f}")

    if failures:
        detail = "; ".join(failures)
        streets_used = sorted(street_names)
        return TestResult(
            case.name,
            passed=False,
            message=f"{detail}. Streets used: {streets_used}",
        )

    return TestResult(case.name, passed=True, message="OK")


def run_route_tests(graph_path: str, logger: logging.Logger = logger) -> list[TestResult]:
    """Load the graph and run all route quality tests.

    This is the function you call from an Airflow PythonOperator.
    Raises RuntimeError if any test fails, so the Airflow task fails too.

    Returns the list of TestResults for logging/inspection.
    """
    graph_path = Path(graph_path)
    logger.info("Loading graph from %s", graph_path)

    compressed = graph_path.read_bytes()
    G = pickle.loads(gzip.decompress(compressed))
    kdtree, node_ids = build_kdtree(G)

    logger.info(
        "Graph loaded: %d nodes, %d edges. Running %d test cases.",
        G.number_of_nodes(),
        G.number_of_edges(),
        len(TEST_CASES),
    )

    results = []
    for case in TEST_CASES:
        result = run_single_test(G, kdtree, node_ids, case)
        status = "PASS" if result.passed else "FAIL"
        logger.info("[%s] %s — %s", status, result.case_name, result.message)
        results.append(result)

    failed = [r for r in results if not r.passed]
    if failed:
        summary = "\n".join(f"  - {r.case_name}: {r.message}" for r in failed)
        raise RuntimeError(f"{len(failed)}/{len(results)} route quality tests failed:\n{summary}")

    logger.info("All %d route quality tests passed.", len(results))
    return results
