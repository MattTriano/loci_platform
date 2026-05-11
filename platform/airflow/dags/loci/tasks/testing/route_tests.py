# loci_platform/platform/airflow/dags/loci/tasks/testing/route_tests.py
"""
Route quality tests for the bike stress routing graph.

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


@dataclass
class TestResult:
    case_name: str
    passed: bool
    message: str


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------


def _street_name_matches(route_names: set[str], pattern: str) -> bool:
    """Check if any street name in the route contains the pattern (case-insensitive)."""
    pattern_lower = pattern.lower()
    return any(pattern_lower in name.lower() for name in route_names)


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


def run_route_tests(
    graph_path: str,
    test_cases: list[RouteTestCase],
    logger: logging.Logger = logger,
) -> list[TestResult]:
    """Load the graph and run the given route quality tests.

    Raises RuntimeError if any test fails, so the Airflow task fails too.
    Returns the list of TestResults for logging/inspection.

    If test_cases is empty, logs a warning and returns an empty list. This
    lets cities adopt the bike-map DAG without route tests on day one.
    """
    if not test_cases:
        logger.warning("No route test cases provided; skipping route quality tests")
        return []

    graph_path = Path(graph_path)
    logger.info("Loading graph from %s", graph_path)

    compressed = graph_path.read_bytes()
    G = pickle.loads(gzip.decompress(compressed))
    kdtree, node_ids = build_kdtree(G)

    logger.info(
        "Graph loaded: %d nodes, %d edges. Running %d test cases.",
        G.number_of_nodes(),
        G.number_of_edges(),
        len(test_cases),
    )

    results = []
    for case in test_cases:
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
