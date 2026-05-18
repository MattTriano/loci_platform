# loci_platform/platform/airflow/dags/loci/tasks/testing/route_tests.py
"""
Route quality tests for the bike stress routing graph.

Drives the routing-cli binary in fixtures mode against a freshly built
graph file, then asserts each result satisfies the test case's
expectations.

The CLI is invoked as a single subprocess: stdin gets JSONL route
requests, stdout returns JSONL results, exit code is 0 if every input
produced a routable result. This keeps the Python side free of any
routing logic — we just shape inputs and check outputs.

Usage from an Airflow DAG:

    test_task = run_tests(
        graph_path="/tmp/routing_graph.bin.gz",
        test_cases=[RouteTestCase(...)],
        task_logger=task_logger,
    )

Each test case is the same dataclass the legacy code used, so existing
DAG definitions (refresh_bike_map_chicago, etc.) need no changes.
"""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Path inside the Airflow worker where chunk 7's compose mount lands.
ROUTING_CLI_PATH = Path("/opt/airflow/build/routing/routing-cli")

# How long any single CLI invocation may take. Generous since the
# fixtures mode loads the graph once and runs every case in one shot.
CLI_TIMEOUT_SEC = 300


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


def run_route_tests(
    graph_path: str,
    test_cases: list[RouteTestCase],
    logger: logging.Logger = logger,
) -> list[TestResult]:
    """Run route quality tests against the graph at graph_path.

    Raises RuntimeError if any test fails, so the Airflow task fails too.
    Returns the list of TestResults for logging/inspection.

    Empty `test_cases` → warning + empty list. Lets a city adopt the
    DAG without route tests on day one (matches the legacy behavior).
    """
    if not test_cases:
        logger.warning("No route test cases provided; skipping route quality tests")
        return []

    if not ROUTING_CLI_PATH.is_file():
        raise RuntimeError(
            f"routing-cli not found at {ROUTING_CLI_PATH}. "
            f"Has `make build` been run in services/routing/, and is the "
            f"build-output volume mounted? See podman/orchestration/CHUNK7-NOTES.md."
        )

    graph_path = Path(graph_path)
    if not graph_path.is_file():
        raise RuntimeError(f"graph file not found at {graph_path}")

    logger.info("Running %d route quality tests against %s", len(test_cases), graph_path)

    # Drive the CLI: one process, fixtures-mode stdin, JSONL stdout.
    cli_output = _invoke_cli_fixtures(graph_path, test_cases, logger)
    results = _check_assertions(cli_output, test_cases, logger)

    # Log each result and then raise on any failure so the task fails.
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        logger.info("[%s] %s — %s", status, r.case_name, r.message)

    failed = [r for r in results if not r.passed]
    if failed:
        summary = "\n".join(f"  - {r.case_name}: {r.message}" for r in failed)
        raise RuntimeError(f"{len(failed)}/{len(results)} route quality tests failed:\n{summary}")

    logger.info("All %d route quality tests passed.", len(results))
    return results


def _invoke_cli_fixtures(
    graph_path: Path,
    test_cases: list[RouteTestCase],
    logger: logging.Logger,
) -> list[dict]:
    """Run the CLI once in fixtures mode and return parsed JSONL lines.

    Each line of stdout corresponds (positionally and by `request.name`)
    to one input case.
    """
    # Build the JSONL input. We include `name` on each request so the
    # output can be matched back to the case by name even if the CLI
    # ever reorders results.
    input_lines = []
    for case in test_cases:
        input_lines.append(
            json.dumps(
                {
                    "name": case.name,
                    "origin": [case.origin[0], case.origin[1]],
                    "destination": [case.destination[0], case.destination[1]],
                }
            )
        )
    stdin_payload = "\n".join(input_lines) + "\n"

    proc = subprocess.run(
        [
            str(ROUTING_CLI_PATH),
            "--graph",
            str(graph_path),
            "--fixtures",
        ],
        input=stdin_payload,
        capture_output=True,
        text=True,
        timeout=CLI_TIMEOUT_SEC,
        check=False,
    )

    # Exit code 0 = all OK, 5 = some fixtures had bad input or no route.
    # The latter is *not* a runner failure — those become test failures
    # downstream. Anything else is a real subprocess error.
    if proc.returncode not in (0, 5):
        logger.error("routing-cli stderr: %s", proc.stderr)
        raise RuntimeError(
            f"routing-cli failed with exit code {proc.returncode}. stderr: {proc.stderr.strip()}"
        )

    if proc.stderr.strip():
        # Non-fatal stderr (e.g. tracing logs) — surface it for context.
        logger.debug("routing-cli stderr: %s", proc.stderr.strip())

    output_lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if len(output_lines) != len(test_cases):
        raise RuntimeError(
            f"routing-cli returned {len(output_lines)} result lines for "
            f"{len(test_cases)} input cases; expected exact 1:1 correspondence"
        )

    parsed = []
    for i, line in enumerate(output_lines):
        try:
            parsed.append(json.loads(line))
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"routing-cli returned non-JSON on line {i + 1}: {line!r} ({e})"
            ) from e
    return parsed


def _check_assertions(
    cli_results: list[dict],
    test_cases: list[RouteTestCase],
    logger: logging.Logger,
) -> list[TestResult]:
    """Pair CLI results with test cases and check each case's assertions."""
    # Build a name → result index so we can match positionally OR by name.
    # The CLI preserves input order, so positional matching is the primary
    # mechanism; matching by name is a defensive check.
    results = []
    for case, cli_result in zip(test_cases, cli_results, strict=True):
        echoed_name = cli_result.get("request", {}).get("name")
        if echoed_name != case.name:
            results.append(
                TestResult(
                    case.name,
                    passed=False,
                    message=(
                        f"CLI output drift: expected request.name={case.name!r}, "
                        f"got {echoed_name!r}"
                    ),
                )
            )
            continue

        results.append(_check_single_assertions(case, cli_result))
    return results


def _check_single_assertions(case: RouteTestCase, cli_result: dict) -> TestResult:
    status = cli_result.get("status")
    if status == "no_route":
        return TestResult(
            case.name, passed=False, message="No route found between origin and destination"
        )
    if status == "bad_request":
        return TestResult(
            case.name,
            passed=False,
            message=f"CLI rejected request: {cli_result.get('error', 'unknown')}",
        )
    if status != "ok":
        return TestResult(
            case.name,
            passed=False,
            message=f"Unexpected CLI status {status!r}: {cli_result.get('error', '')}",
        )

    route = cli_result.get("result")
    if not isinstance(route, dict):
        return TestResult(case.name, passed=False, message="CLI result missing 'result' object")

    street_names = _collect_street_names(route)
    failures = []

    for required in case.must_use:
        if not _street_name_matches(street_names, required):
            failures.append(f"Expected route to use {required!r} but it didn't")

    for forbidden in case.must_avoid:
        if _street_name_matches(street_names, forbidden):
            failures.append(f"Route should avoid {forbidden!r} but it was used")

    if case.max_cost is not None:
        total_cost = float(route.get("total_cost", float("inf")))
        if total_cost > case.max_cost:
            failures.append(f"Cost {total_cost:.1f} exceeds max {case.max_cost:.1f}")

    if failures:
        detail = "; ".join(failures)
        streets_used = sorted(street_names)
        return TestResult(
            case.name,
            passed=False,
            message=f"{detail}. Streets used: {streets_used}",
        )
    return TestResult(case.name, passed=True, message="OK")


def _collect_street_names(route: dict) -> set[str]:
    """Extract the set of non-null street names from a CLI route result."""
    names = set()
    for seg in route.get("segments", []):
        name = seg.get("name")
        if name:
            names.add(name)
    return names


def _street_name_matches(route_names: set[str], pattern: str) -> bool:
    """Case-insensitive substring match against the route's street names."""
    pattern_lower = pattern.lower()
    return any(pattern_lower in name.lower() for name in route_names)
