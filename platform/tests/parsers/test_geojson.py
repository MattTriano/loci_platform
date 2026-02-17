"""Tests for parse_geojson."""

import json
from pathlib import Path

import pytest

from parsers.geojson import parse_geojson


def _write_geojson(tmp_path: Path, features: list[dict]) -> Path:
    """Write a minimal GeoJSON FeatureCollection to a temp file."""
    filepath = tmp_path / "test.geojson"
    filepath.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    return filepath


def _make_feature(
    props: dict | None = None,
    geometry: dict | None = None,
) -> dict:
    """Build a GeoJSON feature with sensible defaults."""
    return {
        "type": "Feature",
        "properties": props or {"name": "test", "value": 42},
        "geometry": geometry or {"type": "Point", "coordinates": [-87.6, 41.8]},
    }


def _collect_all_rows(batches) -> list[dict]:
    """Drain the batch iterator into a flat list of rows."""
    return [row for batch in batches for row in batch]


# ── Basic parsing ──────────────────────────────────────────────────


class TestBasicParsing:
    def test_single_feature(self, tmp_path):

        filepath = _write_geojson(tmp_path, [_make_feature()])
        batches, result = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert len(rows) == 1
        assert result.features_parsed == 1
        assert result.features_failed == 0

    def test_properties_are_extracted(self, tmp_path):

        props = {"city": "Chicago", "count": 100}
        filepath = _write_geojson(tmp_path, [_make_feature(props=props)])
        batches, result = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert rows[0]["city"] == "Chicago"
        assert rows[0]["count"] == 100

    def test_geometry_is_wkb_hex(self, tmp_path):
        from shapely import wkb

        filepath = _write_geojson(tmp_path, [_make_feature()])
        batches, _ = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        # Should be a valid WKB hex string that Shapely can read back
        geom = wkb.loads(rows[0]["geom"], hex=True)
        assert geom.geom_type == "Point"
        assert round(geom.x, 1) == -87.6
        assert round(geom.y, 1) == 41.8

    def test_geometry_column_name_is_configurable(self, tmp_path):

        filepath = _write_geojson(tmp_path, [_make_feature()])
        batches, _ = parse_geojson(filepath, geometry_column="location")
        rows = _collect_all_rows(batches)

        assert "location" in rows[0]
        assert "geom" not in rows[0]

    def test_empty_feature_collection(self, tmp_path):

        filepath = _write_geojson(tmp_path, [])
        batches, result = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert rows == []
        assert result.features_parsed == 0


# ── Batching ───────────────────────────────────────────────────────


class TestBatching:
    def test_rows_split_into_batches(self, tmp_path):

        features = [_make_feature(props={"id": i}) for i in range(7)]
        filepath = _write_geojson(tmp_path, features)
        batches, result = parse_geojson(filepath, geometry_column="geom", batch_size=3)
        batch_list = list(batches)

        assert len(batch_list) == 3  # 3 + 3 + 1
        assert len(batch_list[0]) == 3
        assert len(batch_list[1]) == 3
        assert len(batch_list[2]) == 1
        assert result.features_parsed == 7

    def test_exact_multiple_of_batch_size(self, tmp_path):

        features = [_make_feature(props={"id": i}) for i in range(6)]
        filepath = _write_geojson(tmp_path, features)
        batches, _ = parse_geojson(filepath, geometry_column="geom", batch_size=3)
        batch_list = list(batches)

        assert len(batch_list) == 2
        assert all(len(b) == 3 for b in batch_list)


# ── Column discovery and handling ──────────────────────────────────


class TestColumnHandling:
    def test_columns_locked_from_first_feature(self, tmp_path):
        """Columns are determined by the first valid feature's properties."""

        features = [
            _make_feature(props={"a": 1, "b": 2}),
            _make_feature(props={"a": 3, "b": 4}),
        ]
        filepath = _write_geojson(tmp_path, features)
        batches, _ = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert set(rows[0].keys()) == {"a", "b", "geom"}
        assert set(rows[1].keys()) == {"a", "b", "geom"}

    def test_missing_properties_become_none(self, tmp_path):
        """Features missing a property that the first feature had get None."""

        features = [
            _make_feature(props={"a": 1, "b": 2}),
            _make_feature(props={"a": 3}),  # missing "b"
        ]
        filepath = _write_geojson(tmp_path, features)
        batches, _ = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert rows[1]["a"] == 3
        assert rows[1]["b"] is None

    def test_unknown_properties_are_tracked(self, tmp_path):
        """Extra properties not in the first feature are omitted but recorded."""

        features = [
            _make_feature(props={"a": 1}),
            _make_feature(props={"a": 2, "extra": "surprise"}),
            _make_feature(props={"a": 3, "extra": "again"}),
        ]
        filepath = _write_geojson(tmp_path, features)
        batches, result = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        # Extra property not in rows
        assert "extra" not in rows[1]
        # But tracked in result
        assert result.unknown_properties == {"extra": 2}


# ── Error handling ─────────────────────────────────────────────────


class TestErrorHandling:
    def test_feature_without_geometry_is_skipped(self, tmp_path):

        features = [
            _make_feature(),
            {"type": "Feature", "properties": {"a": 1}, "geometry": None},
            _make_feature(props={"name": "after"}),
        ]
        filepath = _write_geojson(tmp_path, features)
        batches, result = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert len(rows) == 2
        assert result.features_parsed == 2
        assert result.features_failed == 1
        assert result.failures[0]["index"] == 1
        assert "no geometry" in result.failures[0]["error"].lower()

    def test_invalid_geometry_is_skipped(self, tmp_path):

        features = [
            _make_feature(),
            _make_feature(geometry={"type": "BadType", "coordinates": []}),
        ]
        filepath = _write_geojson(tmp_path, features)
        batches, result = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert len(rows) == 1
        assert result.features_failed == 1

    def test_valid_features_still_parsed_after_failures(self, tmp_path):
        """Failures don't stop subsequent valid features from being parsed."""

        features = [
            {"type": "Feature", "properties": {"a": 1}, "geometry": None},
            _make_feature(props={"a": 2}),
            {"type": "Feature", "properties": {"a": 3}, "geometry": None},
            _make_feature(props={"a": 4}),
        ]
        filepath = _write_geojson(tmp_path, features)
        batches, result = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert result.features_parsed == 2
        assert result.features_failed == 2
        assert rows[0]["a"] == 2
        assert rows[1]["a"] == 4


# ── Geometry types ─────────────────────────────────────────────────


class TestGeometryTypes:
    @pytest.mark.parametrize(
        "geom_type, coordinates",
        [
            ("Point", [-87.6, 41.8]),
            ("LineString", [[-87.6, 41.8], [-87.7, 41.9]]),
            (
                "Polygon",
                [[[-87.6, 41.8], [-87.7, 41.8], [-87.7, 41.9], [-87.6, 41.8]]],
            ),
            ("MultiPoint", [[-87.6, 41.8], [-87.7, 41.9]]),
        ],
    )
    def test_various_geometry_types(self, tmp_path, geom_type, coordinates):
        from shapely import wkb

        geom = {"type": geom_type, "coordinates": coordinates}
        filepath = _write_geojson(tmp_path, [_make_feature(geometry=geom)])
        batches, result = parse_geojson(filepath, geometry_column="geom")
        rows = _collect_all_rows(batches)

        assert result.features_parsed == 1
        parsed_geom = wkb.loads(rows[0]["geom"], hex=True)
        assert parsed_geom.geom_type == geom_type
