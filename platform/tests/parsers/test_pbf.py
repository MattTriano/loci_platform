"""
Tests for pbf_parser.parse_pbf.

Each test creates a real .osm.pbf fixture using pyosmium's SimpleWriter,
then runs it through parse_pbf and checks the output. This exercises the
full read path (pyosmium → shapely → WKB → row dicts) with no mocking.
"""

from __future__ import annotations

import json
from pathlib import Path

import osmium
import pytest
from loci.parsers.pbf import parse_pbf
from osmium.osm.mutable import Node, Relation, Way
from shapely import wkb as shapely_wkb

# ---------------------------------------------------------------------------
# Fixtures: generate real PBF files in a temp directory
# ---------------------------------------------------------------------------


@pytest.fixture()
def pbf_with_nodes(tmp_path: Path) -> Path:
    """PBF containing a handful of tagged and untagged nodes."""
    fp = tmp_path / "nodes.osm.pbf"
    with osmium.SimpleWriter(str(fp)) as w:
        # A café in Chicago
        w.add_node(
            Node(
                id=1001,
                location=(-87.6298, 41.8781),
                tags={"name": "Test Café", "amenity": "cafe"},
                version=3,
                changeset=12345,
                timestamp="2024-06-15T12:00:00Z",
            )
        )
        # A fire hydrant with no tags
        w.add_node(
            Node(
                id=1002,
                location=(-87.6300, 41.8785),
                tags={},
                version=1,
                changeset=12346,
                timestamp="2024-06-16T08:30:00Z",
            )
        )
        # A node with unicode tags
        w.add_node(
            Node(
                id=1003,
                location=(139.6917, 35.6895),
                tags={"name": "東京タワー", "name:en": "Tokyo Tower", "tourism": "attraction"},
                version=5,
                changeset=99999,
                timestamp="2024-07-01T00:00:00Z",
            )
        )
    return fp


@pytest.fixture()
def pbf_with_ways(tmp_path: Path) -> Path:
    """PBF with nodes and ways — including open and closed ways."""
    fp = tmp_path / "ways.osm.pbf"
    with osmium.SimpleWriter(str(fp)) as w:
        # Nodes forming a square (for a closed way / building)
        w.add_node(Node(id=1, location=(-87.630, 41.878)))
        w.add_node(Node(id=2, location=(-87.629, 41.878)))
        w.add_node(Node(id=3, location=(-87.629, 41.879)))
        w.add_node(Node(id=4, location=(-87.630, 41.879)))

        # Nodes for an open way (a road)
        w.add_node(Node(id=10, location=(-87.635, 41.880)))
        w.add_node(Node(id=11, location=(-87.634, 41.881)))
        w.add_node(Node(id=12, location=(-87.633, 41.882)))

        # Closed way: a building (first == last node)
        w.add_way(
            Way(
                id=2001,
                nodes=[1, 2, 3, 4, 1],
                tags={"building": "yes", "name": "Test Building"},
                version=2,
                changeset=50000,
                timestamp="2024-03-10T14:00:00Z",
            )
        )

        # Open way: a road
        w.add_way(
            Way(
                id=2002,
                nodes=[10, 11, 12],
                tags={"highway": "residential", "name": "Main St"},
                version=1,
                changeset=50001,
                timestamp="2024-03-11T09:00:00Z",
            )
        )
    return fp


@pytest.fixture()
def pbf_with_relations(tmp_path: Path) -> Path:
    """PBF with relations of various types."""
    fp = tmp_path / "relations.osm.pbf"
    with osmium.SimpleWriter(str(fp)) as w:
        # Multipolygon relation (e.g. a lake with an island)
        w.add_relation(
            Relation(
                id=3001,
                members=[
                    ("w", 100, "outer"),
                    ("w", 101, "inner"),
                ],
                tags={"type": "multipolygon", "natural": "water", "name": "Test Lake"},
                version=4,
                changeset=77777,
                timestamp="2024-05-20T18:00:00Z",
            )
        )

        # Route relation (a bus route)
        w.add_relation(
            Relation(
                id=3002,
                members=[
                    ("w", 200, ""),
                    ("w", 201, ""),
                    ("n", 300, "stop"),
                    ("n", 301, "stop"),
                ],
                tags={"type": "route", "route": "bus", "ref": "42"},
                version=1,
                changeset=77778,
                timestamp="2024-05-21T10:00:00Z",
            )
        )

        # Relation with no tags
        w.add_relation(
            Relation(
                id=3003,
                members=[("n", 400, "")],
                tags={},
                version=1,
                changeset=77779,
                timestamp="2024-05-22T06:00:00Z",
            )
        )
    return fp


@pytest.fixture()
def pbf_with_many_nodes(tmp_path: Path) -> Path:
    """PBF with enough nodes to exercise batching."""
    fp = tmp_path / "many_nodes.osm.pbf"
    with osmium.SimpleWriter(str(fp)) as w:
        for i in range(1, 101):
            w.add_node(
                Node(
                    id=i,
                    location=(float(i) * 0.001, float(i) * 0.001),
                    tags={"index": str(i)},
                )
            )
    return fp


@pytest.fixture()
def empty_pbf(tmp_path: Path) -> Path:
    """PBF with no elements at all."""
    fp = tmp_path / "empty.osm.pbf"
    with osmium.SimpleWriter(str(fp)) as w:
        pass  # write header only
    return fp


@pytest.fixture()
def pbf_with_way_missing_nodes(tmp_path: Path) -> Path:
    """PBF with a way whose referenced nodes are not in the file."""
    fp = tmp_path / "missing_nodes.osm.pbf"
    with osmium.SimpleWriter(str(fp)) as w:
        # Only define one node; the way references three.
        w.add_node(Node(id=1, location=(-87.630, 41.878)))
        w.add_way(
            Way(
                id=5001,
                nodes=[1, 999, 998],
                tags={"highway": "path"},
            )
        )
    return fp


# ---------------------------------------------------------------------------
# Tests: nodes
# ---------------------------------------------------------------------------

NODE_ROW_KEYS = {"osm_id", "tags", "osm_timestamp", "osm_version", "osm_changeset", "geom"}
WAY_ROW_KEYS = {"osm_id", "tags", "osm_timestamp", "osm_version", "osm_changeset", "geom"}
RELATION_ROW_KEYS = {"osm_id", "tags", "osm_timestamp", "osm_version", "osm_changeset", "members"}


class TestParseNodes:
    def test_basic_node_parsing(self, pbf_with_nodes: Path):
        batches, result = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        assert result.features_parsed == 3
        assert result.features_failed == 0
        assert result.element_type == "nodes"
        assert len(rows) == 3

    def test_node_row_schema(self, pbf_with_nodes: Path):
        batches, result = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        row = rows[0]
        assert set(row.keys()) == NODE_ROW_KEYS
        assert row["osm_id"] == 1001

    def test_node_geometry_is_valid_wkb_point(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        for row in rows:
            geom = shapely_wkb.loads(row["geom"], hex=True)
            assert geom.geom_type == "Point"

    def test_node_coordinates_roundtrip(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        cafe = rows[0]
        geom = shapely_wkb.loads(cafe["geom"], hex=True)
        assert geom.x == pytest.approx(-87.6298, abs=1e-4)
        assert geom.y == pytest.approx(41.8781, abs=1e-4)

    def test_node_tags_are_sorted_json(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        cafe = rows[0]
        tags = json.loads(cafe["tags"])
        assert tags == {"amenity": "cafe", "name": "Test Café"}

        # Verify sorted keys for deterministic hashing
        keys = list(json.loads(cafe["tags"]).keys())
        assert keys == sorted(keys)

    def test_node_empty_tags(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        hydrant = rows[1]
        assert json.loads(hydrant["tags"]) == {}

    def test_node_unicode_tags(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        tower = rows[2]
        tags = json.loads(tower["tags"])
        assert tags["name"] == "東京タワー"
        assert tags["name:en"] == "Tokyo Tower"

    def test_node_custom_geometry_column(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes", geometry_column="the_geom")
        rows = [row for batch in batches for row in batch]
        assert "the_geom" in rows[0]
        assert "geom" not in rows[0]


# ---------------------------------------------------------------------------
# Tests: ways
# ---------------------------------------------------------------------------


class TestParseWays:
    def test_basic_way_parsing(self, pbf_with_ways: Path):
        batches, result = parse_pbf(pbf_with_ways, element_type="ways")
        rows = [row for batch in batches for row in batch]

        assert result.features_parsed == 2
        assert result.features_failed == 0
        assert len(rows) == 2

    def test_way_row_schema(self, pbf_with_ways: Path):
        batches, _ = parse_pbf(pbf_with_ways, element_type="ways")
        rows = [row for batch in batches for row in batch]

        assert set(rows[0].keys()) == WAY_ROW_KEYS

    def test_closed_way_becomes_polygon(self, pbf_with_ways: Path):
        batches, _ = parse_pbf(pbf_with_ways, element_type="ways")
        rows = [row for batch in batches for row in batch]

        building = next(r for r in rows if r["osm_id"] == 2001)
        geom = shapely_wkb.loads(building["geom"], hex=True)
        assert geom.geom_type == "Polygon"

    def test_open_way_becomes_linestring(self, pbf_with_ways: Path):
        batches, _ = parse_pbf(pbf_with_ways, element_type="ways")
        rows = [row for batch in batches for row in batch]

        road = next(r for r in rows if r["osm_id"] == 2002)
        geom = shapely_wkb.loads(road["geom"], hex=True)
        assert geom.geom_type == "LineString"

    def test_way_tags_parsed(self, pbf_with_ways: Path):
        batches, _ = parse_pbf(pbf_with_ways, element_type="ways")
        rows = [row for batch in batches for row in batch]

        road = next(r for r in rows if r["osm_id"] == 2002)
        tags = json.loads(road["tags"])
        assert tags["highway"] == "residential"
        assert tags["name"] == "Main St"

    def test_way_with_missing_nodes_records_failure(self, pbf_with_way_missing_nodes: Path):
        """A way whose node refs can't be resolved should fail gracefully."""
        batches, result = parse_pbf(pbf_with_way_missing_nodes, element_type="ways")
        rows = [row for batch in batches for row in batch]

        # The way has 3 node refs but only 1 is in the file,
        # so it should either fail or produce a degenerate geometry.
        # Either outcome is acceptable as long as the parser doesn't crash.
        total = result.features_parsed + result.features_failed
        assert total >= 1


# ---------------------------------------------------------------------------
# Tests: relations
# ---------------------------------------------------------------------------


class TestParseRelations:
    def test_basic_relation_parsing(self, pbf_with_relations: Path):
        batches, result = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        assert result.features_parsed == 3
        assert result.features_failed == 0
        assert len(rows) == 3

    def test_relation_row_schema(self, pbf_with_relations: Path):
        batches, _ = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        assert set(rows[0].keys()) == RELATION_ROW_KEYS

    def test_relation_has_no_geometry_column(self, pbf_with_relations: Path):
        batches, _ = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        for row in rows:
            assert "geom" not in row

    def test_relation_members_structure(self, pbf_with_relations: Path):
        batches, _ = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        lake = next(r for r in rows if r["osm_id"] == 3001)
        members = json.loads(lake["members"])

        assert len(members) == 2
        assert members[0]["type"] == "w"
        assert members[0]["ref"] == 100
        assert members[0]["role"] == "outer"
        assert members[1]["type"] == "w"
        assert members[1]["ref"] == 101
        assert members[1]["role"] == "inner"

    def test_relation_mixed_member_types(self, pbf_with_relations: Path):
        batches, _ = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        bus = next(r for r in rows if r["osm_id"] == 3002)
        members = json.loads(bus["members"])

        types = {m["type"] for m in members}
        assert types == {"w", "n"}

    def test_relation_empty_role(self, pbf_with_relations: Path):
        batches, _ = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        bus = next(r for r in rows if r["osm_id"] == 3002)
        members = json.loads(bus["members"])

        way_members = [m for m in members if m["type"] == "w"]
        assert all(m["role"] == "" for m in way_members)

    def test_relation_tags(self, pbf_with_relations: Path):
        batches, _ = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        lake = next(r for r in rows if r["osm_id"] == 3001)
        tags = json.loads(lake["tags"])
        assert tags["type"] == "multipolygon"
        assert tags["natural"] == "water"
        assert tags["name"] == "Test Lake"

    def test_relation_empty_tags(self, pbf_with_relations: Path):
        batches, _ = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        bare = next(r for r in rows if r["osm_id"] == 3003)
        assert json.loads(bare["tags"]) == {}


# ---------------------------------------------------------------------------
# Tests: OSM metadata (timestamp, version, changeset)
# ---------------------------------------------------------------------------


class TestOsmMetadataFields:
    def test_node_timestamp(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        cafe = rows[0]
        assert cafe["osm_timestamp"] is not None
        assert "2024-06-15" in cafe["osm_timestamp"]

    def test_node_version_and_changeset(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        cafe = rows[0]
        assert cafe["osm_version"] == 3
        assert cafe["osm_changeset"] == 12345

    def test_way_timestamp(self, pbf_with_ways: Path):
        batches, _ = parse_pbf(pbf_with_ways, element_type="ways")
        rows = [row for batch in batches for row in batch]

        building = next(r for r in rows if r["osm_id"] == 2001)
        assert building["osm_timestamp"] is not None
        assert "2024-03-10" in building["osm_timestamp"]
        assert building["osm_version"] == 2
        assert building["osm_changeset"] == 50000

    def test_relation_timestamp(self, pbf_with_relations: Path):
        batches, _ = parse_pbf(pbf_with_relations, element_type="relations")
        rows = [row for batch in batches for row in batch]

        lake = next(r for r in rows if r["osm_id"] == 3001)
        assert lake["osm_timestamp"] is not None
        assert "2024-05-20" in lake["osm_timestamp"]
        assert lake["osm_version"] == 4
        assert lake["osm_changeset"] == 77777

    def test_missing_timestamp_returns_none(self, pbf_with_many_nodes: Path):
        """Nodes written without explicit timestamps should have None."""
        batches, _ = parse_pbf(pbf_with_many_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        # many_nodes fixture doesn't set timestamps
        assert rows[0]["osm_timestamp"] is None

    def test_all_element_types_have_metadata_fields(
        self,
        pbf_with_nodes,
        pbf_with_ways,
        pbf_with_relations,
    ):
        """Every element type should include all three metadata fields."""
        for fp, etype in [
            (pbf_with_nodes, "nodes"),
            (pbf_with_ways, "ways"),
            (pbf_with_relations, "relations"),
        ]:
            batches, _ = parse_pbf(fp, element_type=etype)
            rows = [row for batch in batches for row in batch]
            assert len(rows) > 0
            for row in rows:
                assert "osm_timestamp" in row
                assert "osm_version" in row
                assert "osm_changeset" in row


# ---------------------------------------------------------------------------
# Tests: batching
# ---------------------------------------------------------------------------


class TestBatching:
    def test_single_batch_when_under_batch_size(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes", batch_size=100)
        batch_list = list(batches)
        assert len(batch_list) == 1
        assert len(batch_list[0]) == 3

    def test_multiple_batches(self, pbf_with_many_nodes: Path):
        batches, result = parse_pbf(pbf_with_many_nodes, element_type="nodes", batch_size=30)
        batch_list = list(batches)

        # 100 nodes / 30 per batch = 4 batches (30, 30, 30, 10)
        assert len(batch_list) == 4
        assert len(batch_list[0]) == 30
        assert len(batch_list[-1]) == 10
        assert result.features_parsed == 100

    def test_batch_size_one(self, pbf_with_nodes: Path):
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes", batch_size=1)
        batch_list = list(batches)
        assert len(batch_list) == 3
        assert all(len(b) == 1 for b in batch_list)


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_file_nodes(self, empty_pbf: Path):
        batches, result = parse_pbf(empty_pbf, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        assert rows == []
        assert result.features_parsed == 0
        assert result.features_failed == 0

    def test_empty_file_ways(self, empty_pbf: Path):
        batches, result = parse_pbf(empty_pbf, element_type="ways")
        rows = [row for batch in batches for row in batch]

        assert rows == []
        assert result.features_parsed == 0

    def test_empty_file_relations(self, empty_pbf: Path):
        batches, result = parse_pbf(empty_pbf, element_type="relations")
        rows = [row for batch in batches for row in batch]

        assert rows == []
        assert result.features_parsed == 0

    def test_invalid_element_type_raises(self, empty_pbf: Path):
        with pytest.raises(ValueError, match="element_type must be one of"):
            parse_pbf(empty_pbf, element_type="changesets")

    def test_parsing_nodes_ignores_ways_and_relations(self, pbf_with_ways: Path):
        """The ways fixture also has nodes; make sure parsing nodes
        doesn't return way data."""
        batches, result = parse_pbf(pbf_with_ways, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        # The fixture has 7 nodes and 2 ways
        assert result.features_parsed == 7
        for row in rows:
            assert "members" not in row

    def test_parsing_relations_from_file_with_only_nodes(self, pbf_with_nodes: Path):
        batches, result = parse_pbf(pbf_with_nodes, element_type="relations")
        rows = [row for batch in batches for row in batch]

        assert rows == []
        assert result.features_parsed == 0

    def test_parse_result_before_iteration(self, pbf_with_nodes: Path):
        """ParseResult should be safe to inspect even before iterating."""
        _, result = parse_pbf(pbf_with_nodes, element_type="nodes")

        # Before consuming the iterator, nothing has been parsed yet.
        assert result.features_parsed == 0
        assert result.features_failed == 0


# ---------------------------------------------------------------------------
# Tests: SCD2 / determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_tags_json_is_deterministic(self, pbf_with_nodes: Path):
        """Parsing the same file twice should produce identical tag strings."""

        def get_tags():
            batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
            return [row["tags"] for batch in batches for row in batch]

        first = get_tags()
        second = get_tags()
        assert first == second

    def test_tags_keys_are_sorted(self, pbf_with_nodes: Path):
        """Tag keys must be sorted for deterministic hashing."""
        batches, _ = parse_pbf(pbf_with_nodes, element_type="nodes")
        rows = [row for batch in batches for row in batch]

        for row in rows:
            tags = json.loads(row["tags"])
            keys = list(tags.keys())
            assert keys == sorted(keys), f"Keys not sorted: {keys}"
