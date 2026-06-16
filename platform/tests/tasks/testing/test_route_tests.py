"""Behavior tests for the route quality test framework (loci.tasks.testing.route_tests).

Design intent — these tests are written to survive a complete rewrite of the
framework's internals. They exercise only the stable contract:

  Inputs:
    - RouteTestCase objects (name, origin, destination, must_use, must_avoid,
      max_cost, must_cross, must_not_cross).
    - Gate objects (two (lat, lon) endpoints).
    - The route geometry/metadata the routing CLI returns.

  Outputs:
    - run_route_tests(...) returns a list of per-case results (each exposing
      .case_name and .passed) when every case passes.
    - run_route_tests(...) raises RuntimeError when any case fails (this is how
      the Airflow task is made to fail), and the failing case's name appears in
      the error.
    - Gate(...) raises ValueError on invalid endpoints.

What these tests deliberately do NOT touch:
    - The intersection algorithm (parametric, orientation, shapely — doesn't
      matter). Geometric behavior is asserted only through must_cross /
      must_not_cross outcomes.
    - Any private helper name or signature.
    - Exact wording of failure messages (only that a failure is surfaced and
      attributed to the right case).

The single external dependency — the routing-cli subprocess — is mocked. The
mock honors the documented CLI protocol (JSONL in, one JSONL result line per
case, echoing request.name, with a status and a result object). Tests that
encode that subprocess protocol specifically are grouped at the bottom and
labeled; if routing ever moves in-process, only that group needs revisiting.

If the package import path differs in your test setup, adjust the import below.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from loci.tasks.testing import route_tests

LOG = logging.getLogger("route_tests_under_test")


# ---------------------------------------------------------------------------
# Geometry fixtures for the canonical route
#
# The canonical route runs due east along latitude 37.770, from longitude
# -122.430 to -122.410, as two named segments. Route coordinates are (lon, lat)
# per the graph format; Gate endpoints are (lat, lon) per RouteTestCase
# convention. Gates below are positioned so their cross/no-cross outcome is
# unambiguous and independent of floating-point edge effects.
# ---------------------------------------------------------------------------

CANONICAL_ROUTE_SEGMENTS = [
    {"name": "Market Street", "coordinates": [[-122.430, 37.770], [-122.420, 37.770]]},
    {"name": "Valencia Street", "coordinates": [[-122.420, 37.770], [-122.410, 37.770]]},
]

# Vertical line at lon -122.415 spanning the route's latitude — clean transversal crossing.
GATE_CROSSES = ((37.768, -122.415), (37.772, -122.415))
# Parallel to the route, well to the north — never crossed.
GATE_PARALLEL_NORTH = ((37.780, -122.430), (37.780, -122.410))
# Vertical line west of the route's start — never reached.
GATE_WEST_OF_START = ((37.768, -122.440), (37.772, -122.440))
# Vertical line exactly through the shared vertex between the two segments —
# the route only grazes its endpoint, which is NOT a transversal crossing.
GATE_THROUGH_VERTEX = ((37.768, -122.420), (37.772, -122.420))
# Collinear with the route line (same latitude, overlapping longitudes).
GATE_COLLINEAR = ((37.770, -122.425), (37.770, -122.415))


# ---------------------------------------------------------------------------
# CLI mock + harness
# ---------------------------------------------------------------------------


def _ok(segments=None, total_cost=300.0):
    """A successful CLI payload for one case."""
    return {
        "status": "ok",
        "result": {
            "segments": segments if segments is not None else CANONICAL_ROUTE_SEGMENTS,
            "total_cost": total_cost,
        },
    }


def _no_route():
    return {"status": "no_route"}


def _fake_cli(payloads, returncode=0, stderr=""):
    """Build a stand-in for subprocess.run that speaks the CLI's JSONL protocol.

    Reads the JSONL request payload from `input`, and for each request emits one
    result line, echoing request.name and attaching the canned payload for that
    name. This mirrors the real CLI's 1:1, order-preserving, name-echoing
    contract without invoking any binary.
    """

    # def _run(cmd, **kwargs):
    #     out_lines = []
    #     for raw in (kwargs.get("input") or "").splitlines():
    #         raw = raw.strip()
    #         if not raw:
    #             continue
    #         name = json.loads(raw)["name"]
    #         payload = dict(payloads[name])
    #         payload["request"] = {"name": name}
    #         out_lines.append(json.dumps(payload))
    #     stdout = "\n".join(out_lines) + ("\n" if out_lines else "")
    #     return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    def _run(cmd, **kwargs):
        out_lines = []
        raw_input = kwargs.get("input") or ""
        for raw in raw_input.splitlines():
            raw = raw.strip()
            if not raw:
                continue
            name = json.loads(raw)["name"]
            if name not in payloads:
                raise AssertionError(
                    f"fake CLI has no payload for case {name!r}; known: {sorted(payloads)}"
                )
            payload = dict(payloads[name])
            payload["request"] = {"name": name}
            out_lines.append(json.dumps(payload))
        stdout = "\n".join(out_lines) + ("\n" if out_lines else "")
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    return _run


@pytest.fixture
def harness(monkeypatch, tmp_path):
    """Return a `run(cases, payloads, **cli_kwargs)` callable.

    Sets up an existing CLI path and graph file (so existence checks pass) and
    patches the subprocess boundary. Everything else is the real framework.
    """
    cli = tmp_path / "routing-cli"
    cli.write_text("")  # exists -> is_file() is True
    graph = tmp_path / "graph.bin.gz"
    graph.write_text("")

    monkeypatch.setattr(route_tests, "ROUTING_CLI_PATH", cli)

    def run(cases, payloads=None, **cli_kwargs):
        payloads = payloads if payloads is not None else {c.name: _ok() for c in cases}
        monkeypatch.setattr(route_tests.subprocess, "run", _fake_cli(payloads, **cli_kwargs))
        return route_tests.run_route_tests(str(graph), cases, logger=LOG)

    return run


def case(
    name="case",
    *,
    must_use=None,
    must_avoid=None,
    max_cost=None,
    must_cross=None,
    must_not_cross=None,
):
    """Build a RouteTestCase with dummy origin/destination (geometry comes from
    the mocked CLI, so origin/destination are irrelevant to these tests)."""
    return route_tests.RouteTestCase(
        name=name,
        origin=(37.770, -122.430),
        destination=(37.770, -122.410),
        must_use=must_use or [],
        must_avoid=must_avoid or [],
        max_cost=max_cost,
        must_cross=must_cross or [],
        must_not_cross=must_not_cross or [],
    )


def gate(endpoints):
    return route_tests.Gate(endpoints[0], endpoints[1])


def result_for(results, name):
    return next(r for r in results if r.case_name == name)


# ===========================================================================
# Gate validation (public input type)
# ===========================================================================


def test_gate_accepts_valid_endpoints():
    g = route_tests.Gate((37.77, -122.42), (37.78, -122.41))
    assert g is not None


def test_gate_rejects_swapped_lat_lon():
    # (lon, lat) passed where (lat, lon) is expected: -122 is not a valid latitude.
    with pytest.raises(ValueError):
        route_tests.Gate((-122.42, 37.77), (-122.41, 37.78))


def test_gate_rejects_out_of_range_longitude():
    with pytest.raises(ValueError):
        route_tests.Gate((37.77, -200.0), (37.78, -122.41))


def test_gate_rejects_zero_length():
    with pytest.raises(ValueError):
        route_tests.Gate((37.77, -122.42), (37.77, -122.42))


# ===========================================================================
# Baseline + output contract
# ===========================================================================


def test_case_with_no_assertions_passes(harness):
    results = harness([case("baseline")])
    assert result_for(results, "baseline").passed is True


def test_all_pass_returns_one_result_per_case(harness):
    cases = [case("a"), case("b"), case("c")]
    results = harness(cases)
    assert {r.case_name for r in results} == {"a", "b", "c"}
    assert all(r.passed for r in results)


def test_empty_test_cases_returns_empty_and_does_not_raise(harness):
    assert harness([]) == []


# ===========================================================================
# Street-name assertions (must_use / must_avoid)
# ===========================================================================


def test_must_use_passes_when_street_present(harness):
    results = harness([case("c", must_use=["Valencia"])])
    assert result_for(results, "c").passed is True


def test_must_use_fails_when_street_absent(harness):
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", must_use=["Folsom"])])


def test_must_avoid_passes_when_street_absent(harness):
    results = harness([case("c", must_avoid=["Folsom"])])
    assert result_for(results, "c").passed is True


def test_must_avoid_fails_when_street_present(harness):
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", must_avoid=["Market"])])


def test_street_matching_is_case_insensitive_substring(harness):
    # "valencia" should match "Valencia Street".
    results = harness([case("c", must_use=["valencia"])])
    assert result_for(results, "c").passed is True


# ===========================================================================
# max_cost tripwire
# ===========================================================================


def test_max_cost_passes_when_within_bound(harness):
    results = harness([case("c", max_cost=500.0)], {"c": _ok(total_cost=300.0)})
    assert result_for(results, "c").passed is True


def test_max_cost_fails_when_exceeded(harness):
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", max_cost=100.0)], {"c": _ok(total_cost=300.0)})


# ===========================================================================
# Geometric gate assertions — behavior only, no coupling to the crossing math
# ===========================================================================


def test_must_cross_passes_when_route_crosses_gate(harness):
    results = harness([case("c", must_cross=[gate(GATE_CROSSES)])])
    assert result_for(results, "c").passed is True


def test_must_cross_fails_when_gate_is_parallel_and_uncrossed(harness):
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", must_cross=[gate(GATE_PARALLEL_NORTH)])])


def test_must_cross_fails_when_gate_is_beyond_the_route(harness):
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", must_cross=[gate(GATE_WEST_OF_START)])])


def test_must_not_cross_passes_when_route_avoids_gate(harness):
    results = harness([case("c", must_not_cross=[gate(GATE_PARALLEL_NORTH)])])
    assert result_for(results, "c").passed is True


def test_must_not_cross_fails_when_route_crosses_gate(harness):
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", must_not_cross=[gate(GATE_CROSSES)])])


# --- transversal-only semantics (a deliberate design choice, asserted as behavior) ---
# These encode the decision that a graze (touching an endpoint or running
# collinear) is NOT a crossing. If that semantic is ever changed on purpose,
# these are the tests that should change with it.


def test_grazing_a_gate_at_a_vertex_is_not_a_crossing(harness):
    # The route only touches GATE_THROUGH_VERTEX at the shared vertex between
    # its two segments — not a transversal crossing, so must_cross must fail.
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", must_cross=[gate(GATE_THROUGH_VERTEX)])])


def test_running_collinear_with_a_gate_is_not_a_crossing(harness):
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", must_cross=[gate(GATE_COLLINEAR)])])


# ===========================================================================
# Combined / regression-style assertions (the elevation use case)
# ===========================================================================


def test_dodge_pattern_crosses_intended_corridor_and_avoids_the_other(harness):
    # The shape of a real elevation regression test: require the route to pass
    # through the intended (flat) corridor AND stay out of the (steep) one.
    results = harness(
        [
            case(
                "dodge",
                must_cross=[gate(GATE_CROSSES)],
                must_not_cross=[gate(GATE_PARALLEL_NORTH)],
            )
        ]
    )
    assert result_for(results, "dodge").passed is True


def test_geometric_and_name_assertions_compose(harness):
    results = harness(
        [case("c", must_use=["Valencia"], must_avoid=["Folsom"], must_cross=[gate(GATE_CROSSES)])]
    )
    assert result_for(results, "c").passed is True


def test_any_failed_assertion_in_a_case_fails_the_case(harness):
    # must_cross satisfied, but must_avoid violated -> the case fails overall.
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c", must_cross=[gate(GATE_CROSSES)], must_avoid=["Market"])])


# ===========================================================================
# Multi-case failure attribution
# ===========================================================================


def test_one_failing_case_among_passing_ones_raises_and_names_the_failure(harness):
    cases = [
        case("good_a"),
        case("bad_one", must_use=["Nonexistent Ave"]),
        case("good_b"),
    ]
    with pytest.raises(RuntimeError) as exc:
        harness(cases)
    # Behavior: the failing case is identifiable; we don't assert exact wording.
    assert "bad_one" in str(exc.value)


# ===========================================================================
# Route-level status handling
# ===========================================================================


def test_no_route_status_is_a_failure(harness):
    with pytest.raises(RuntimeError, match="c"):
        harness([case("c")], {"c": _no_route()})


# ===========================================================================
# CLI subprocess-protocol boundary
#
# These encode the contract with the external routing-cli process specifically
# (exit codes, 1:1 line correspondence). They are the only tests tied to the
# subprocess mechanism; if routing ever moves in-process, revisit just these.
# ===========================================================================


def test_unexpected_cli_exit_code_is_a_runner_error(harness):
    # Exit codes other than 0 / 5 mean the CLI itself failed, which is a runner
    # error (RuntimeError), distinct from a route test failing its assertions.
    with pytest.raises(RuntimeError):
        harness([case("c")], returncode=2)


def test_cli_result_count_mismatch_is_a_runner_error(monkeypatch, tmp_path):
    cli = tmp_path / "routing-cli"
    cli.write_text("")
    graph = tmp_path / "graph.bin.gz"
    graph.write_text("")
    monkeypatch.setattr(route_tests, "ROUTING_CLI_PATH", cli)

    # CLI returns zero result lines for one input case -> 1:1 contract violated.
    def _bad_run(cmd, **kwargs):
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(route_tests.subprocess, "run", _bad_run)

    with pytest.raises(RuntimeError):
        route_tests.run_route_tests(str(graph), [case("c")], logger=LOG)
