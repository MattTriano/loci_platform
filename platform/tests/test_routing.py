"""
Tests for left-turn detection in the routing module.

These guard against regression of the cross-product sign bug in
_is_left_turn, where right turns were being classified as left turns
in a coordinate system with x = longitude (east) and y = latitude
(north).

Geometry conventions used in these tests:
  - x = longitude, increases eastward
  - y = latitude, increases northward
  - "north" of a node is +y, "east" is +x, etc.

Bearings are computed by routing._bearing as atan2(Δx, Δy), which is
the compass-bearing convention (clockwise from north). The tests don't
depend on which convention is used internally — they only construct
graph geometries and ask whether the resulting turn is classified as
left or right.
"""

from __future__ import annotations

import math

import networkx as nx
import pytest
from loci.routing import (
    _bearing,
    _is_left_turn,
    _turn_angle_deg,
    compute_left_turn_penalty,
    get_intersection_nodes,
)


# =====================================================================
# Helpers
# =====================================================================
def _bearing_from_compass(degrees_from_north: float) -> float:
    """Convert a compass bearing in degrees to the radian value that
    routing._bearing would produce for an edge heading in that direction.

    routing._bearing returns atan2(Δx, Δy), which equals the compass
    bearing (clockwise from north) in radians.
    """
    return math.radians(degrees_from_north)


# =====================================================================
# _is_left_turn: direct unit tests
# =====================================================================
# These are the core regression tests. Each case picks an in-bearing
# and an out-bearing and asserts the expected handedness.
#
# Mental model: stand at the intersection facing the direction you came
# FROM (bearing_in points away from you, toward where you're going).
# Then bearing_out is the direction you head next. A left turn rotates
# counter-clockwise when viewed from above (north up).


class TestIsLeftTurnCardinal:
    """Cardinal-direction turns where the answer is unambiguous."""

    def test_north_then_west_is_left(self):
        # Heading north, then turning to head west: classic left turn.
        bearing_in = _bearing_from_compass(0)  # north
        bearing_out = _bearing_from_compass(270)  # west
        assert _is_left_turn(bearing_in, bearing_out) is True

    def test_north_then_east_is_right(self):
        # Heading north, then turning to head east: right turn.
        bearing_in = _bearing_from_compass(0)  # north
        bearing_out = _bearing_from_compass(90)  # east
        assert _is_left_turn(bearing_in, bearing_out) is False

    def test_east_then_north_is_left(self):
        bearing_in = _bearing_from_compass(90)  # east
        bearing_out = _bearing_from_compass(0)  # north
        assert _is_left_turn(bearing_in, bearing_out) is True

    def test_east_then_south_is_right(self):
        bearing_in = _bearing_from_compass(90)  # east
        bearing_out = _bearing_from_compass(180)  # south
        assert _is_left_turn(bearing_in, bearing_out) is False

    def test_south_then_east_is_left(self):
        bearing_in = _bearing_from_compass(180)  # south
        bearing_out = _bearing_from_compass(90)  # east
        assert _is_left_turn(bearing_in, bearing_out) is True

    def test_south_then_west_is_right(self):
        bearing_in = _bearing_from_compass(180)  # south
        bearing_out = _bearing_from_compass(270)  # west
        assert _is_left_turn(bearing_in, bearing_out) is False

    def test_west_then_south_is_left(self):
        bearing_in = _bearing_from_compass(270)  # west
        bearing_out = _bearing_from_compass(180)  # south
        assert _is_left_turn(bearing_in, bearing_out) is True

    def test_west_then_north_is_right(self):
        bearing_in = _bearing_from_compass(270)  # west
        bearing_out = _bearing_from_compass(0)  # north
        assert _is_left_turn(bearing_in, bearing_out) is False


# =====================================================================
# _is_left_turn: bearing built from real graph nodes
# =====================================================================
# These confirm the same property end-to-end through _bearing, in case
# someone ever changes the bearing convention but forgets to update the
# cross-product sign to match. _bearing and _is_left_turn must agree.


@pytest.fixture
def four_way_graph() -> nx.DiGraph:
    """A simple four-way intersection at node 0.

    Geometry (x=lon, y=lat):
        node 1: ( 0,  1)   north
        node 2: ( 1,  0)   east
        node 3: ( 0, -1)   south
        node 4: (-1,  0)   west
        node 0: ( 0,  0)   center
    """
    G = nx.DiGraph()
    G.add_node(0, x=0.0, y=0.0)
    G.add_node(1, x=0.0, y=1.0)
    G.add_node(2, x=1.0, y=0.0)
    G.add_node(3, x=0.0, y=-1.0)
    G.add_node(4, x=-1.0, y=0.0)
    # Add edges in both directions so degree(0) = 8 — comfortably an intersection.
    for n in (1, 2, 3, 4):
        G.add_edge(0, n, stress_cost=1.0)
        G.add_edge(n, 0, stress_cost=1.0)
    return G


class TestIsLeftTurnFromGraph:
    """Build bearings from real node coordinates and check classification."""

    def test_approach_from_south_turn_to_west_is_left(self, four_way_graph):
        # Coming up from node 3 (south) into the intersection at 0,
        # then heading toward node 4 (west): left turn.
        b_in = _bearing(four_way_graph, 3, 0)  # heading north
        b_out = _bearing(four_way_graph, 0, 4)  # heading west
        assert _is_left_turn(b_in, b_out) is True

    def test_approach_from_south_turn_to_east_is_right(self, four_way_graph):
        b_in = _bearing(four_way_graph, 3, 0)  # heading north
        b_out = _bearing(four_way_graph, 0, 2)  # heading east
        assert _is_left_turn(b_in, b_out) is False

    def test_approach_from_north_turn_to_east_is_left(self, four_way_graph):
        # Coming down from node 1 (north), turning toward node 2 (east):
        # standing at the intersection facing south, east is on your left.
        b_in = _bearing(four_way_graph, 1, 0)  # heading south
        b_out = _bearing(four_way_graph, 0, 2)  # heading east
        assert _is_left_turn(b_in, b_out) is True

    def test_approach_from_east_turn_to_south_is_left(self, four_way_graph):
        b_in = _bearing(four_way_graph, 2, 0)  # heading west
        b_out = _bearing(four_way_graph, 0, 3)  # heading south
        assert _is_left_turn(b_in, b_out) is True


# =====================================================================
# _turn_angle_deg: sanity checks
# =====================================================================
# Not the bug under test, but the angle threshold is part of the
# penalty pipeline, so a few smoke checks are cheap insurance.


class TestTurnAngleDeg:
    def test_straight_is_zero(self):
        b = _bearing_from_compass(0)
        assert _turn_angle_deg(b, b) == pytest.approx(0.0)

    def test_right_angle_is_ninety(self):
        b_in = _bearing_from_compass(0)  # north
        b_out = _bearing_from_compass(90)  # east
        assert _turn_angle_deg(b_in, b_out) == pytest.approx(90.0)

    def test_u_turn_is_one_eighty(self):
        b_in = _bearing_from_compass(0)  # north
        b_out = _bearing_from_compass(180)  # south
        assert _turn_angle_deg(b_in, b_out) == pytest.approx(180.0)

    def test_unsigned_left_and_right_match(self):
        # 90° left and 90° right should both report 90°.
        b_in = _bearing_from_compass(0)
        left = _turn_angle_deg(b_in, _bearing_from_compass(270))
        right = _turn_angle_deg(b_in, _bearing_from_compass(90))
        assert left == pytest.approx(right) == pytest.approx(90.0)


# =====================================================================
# compute_left_turn_penalty: end-to-end behavior
# =====================================================================
# Verifies that the penalty fires on left turns and skips right turns,
# straight-throughs, gentle curves, and non-intersection nodes.


@pytest.fixture
def intersection_set(four_way_graph) -> set[int]:
    return get_intersection_nodes(four_way_graph)


class TestComputeLeftTurnPenalty:
    def test_left_turn_at_intersection_gets_penalty(self, four_way_graph, intersection_set):
        # Approach from south (node 3), turn left toward west (node 4).
        # Target edge has highway='residential' so multiplier > 0.
        edge_data = {"highway": "residential"}
        penalty = compute_left_turn_penalty(
            four_way_graph,
            prev=3,
            curr=0,
            next_node=4,
            next_edge_data=edge_data,
            intersection_nodes=intersection_set,
        )
        assert penalty > 0

    def test_right_turn_at_intersection_gets_no_penalty(self, four_way_graph, intersection_set):
        # Approach from south (node 3), turn right toward east (node 2).
        edge_data = {"highway": "residential"}
        penalty = compute_left_turn_penalty(
            four_way_graph,
            prev=3,
            curr=0,
            next_node=2,
            next_edge_data=edge_data,
            intersection_nodes=intersection_set,
        )
        assert penalty == 0.0

    def test_straight_through_gets_no_penalty(self, four_way_graph, intersection_set):
        # Approach from south (node 3), continue north (node 1). Angle = 0.
        edge_data = {"highway": "residential"}
        penalty = compute_left_turn_penalty(
            four_way_graph,
            prev=3,
            curr=0,
            next_node=1,
            next_edge_data=edge_data,
            intersection_nodes=intersection_set,
        )
        assert penalty == 0.0

    def test_non_intersection_gets_no_penalty(self, four_way_graph):
        # If curr is not in the intersection set, no penalty regardless
        # of geometry.
        edge_data = {"highway": "residential"}
        penalty = compute_left_turn_penalty(
            four_way_graph,
            prev=3,
            curr=0,
            next_node=4,
            next_edge_data=edge_data,
            intersection_nodes=set(),  # empty: 0 is not an intersection
        )
        assert penalty == 0.0

    def test_gentle_curve_below_threshold_gets_no_penalty(self, intersection_set):
        # Build a tiny graph with a ~30° bend, well under the 45° threshold.
        G = nx.DiGraph()
        G.add_node(0, x=0.0, y=0.0)
        G.add_node(1, x=0.0, y=-1.0)  # south of center
        # 30° west of north from the center — small bend, "left-ish"
        bend_x = -math.sin(math.radians(30))
        bend_y = math.cos(math.radians(30))
        G.add_node(2, x=bend_x, y=bend_y)
        # Make node 0 an intersection so the angle filter is what gates this.
        G.add_node(3, x=1.0, y=0.0)
        G.add_node(4, x=-1.0, y=0.0)
        for n in (1, 2, 3, 4):
            G.add_edge(0, n, stress_cost=1.0)
            G.add_edge(n, 0, stress_cost=1.0)
        intersections = get_intersection_nodes(G)

        edge_data = {"highway": "residential"}
        penalty = compute_left_turn_penalty(
            G,
            prev=1,
            curr=0,
            next_node=2,
            next_edge_data=edge_data,
            intersection_nodes=intersections,
        )
        assert penalty == 0.0

    def test_target_road_class_scales_penalty(self, four_way_graph, intersection_set):
        # Same left turn, different target highway classes: primary
        # should produce a strictly larger penalty than residential.
        primary = compute_left_turn_penalty(
            four_way_graph,
            prev=3,
            curr=0,
            next_node=4,
            next_edge_data={"highway": "primary"},
            intersection_nodes=intersection_set,
        )
        residential = compute_left_turn_penalty(
            four_way_graph,
            prev=3,
            curr=0,
            next_node=4,
            next_edge_data={"highway": "residential"},
            intersection_nodes=intersection_set,
        )
        assert primary > residential > 0

    def test_cycleway_target_gets_no_penalty(self, four_way_graph, intersection_set):
        # Turning onto a cycleway has no oncoming motor traffic to cross.
        penalty = compute_left_turn_penalty(
            four_way_graph,
            prev=3,
            curr=0,
            next_node=4,
            next_edge_data={"highway": "cycleway"},
            intersection_nodes=intersection_set,
        )
        assert penalty == 0.0
