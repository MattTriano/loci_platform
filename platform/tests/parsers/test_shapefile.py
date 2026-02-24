import zipfile

import fiona
import pytest
from loci.parsers.shapefile import parse_shapefile
from shapely import wkb as shapely_wkb
from shapely.geometry import LineString, Point, Polygon, mapping


@pytest.fixture
def tmp_shapefile(tmp_path):
    """
    Factory fixture: write a minimal shapefile from a list of
    (properties_dict, shapely_geometry) tuples and return the path.
    """

    def _write(features, schema=None, filename="test.shp"):
        filepath = tmp_path / filename
        if schema is None:
            schema = {
                "geometry": "Point",
                "properties": {"NAME": "str", "VALUE": "int"},
            }
        with fiona.open(
            filepath,
            "w",
            driver="ESRI Shapefile",
            schema=schema,
            crs="EPSG:4326",
        ) as dst:
            for props, geom in features:
                dst.write({"properties": props, "geometry": mapping(geom)})
        return filepath

    return _write


@pytest.fixture
def three_points(tmp_shapefile):
    """A shapefile with three simple point features."""
    features = [
        ({"NAME": "Alice", "VALUE": 1}, Point(0, 0)),
        ({"NAME": "Bob", "VALUE": 2}, Point(1, 1)),
        ({"NAME": "Carol", "VALUE": 3}, Point(2, 2)),
    ]
    return tmp_shapefile(features)


# --- Basic parsing ---


def test_parses_features_and_returns_correct_row_dicts(three_points):
    batches, result = parse_shapefile(three_points)
    rows = [row for batch in batches for row in batch]

    assert len(rows) == 3
    assert result.features_parsed == 3
    assert result.features_failed == 0

    assert rows[0]["name"] == "Alice"
    assert rows[0]["value"] == 1
    assert rows[1]["name"] == "Bob"


def test_geometry_column_contains_valid_wkb_hex(three_points):
    batches, result = parse_shapefile(three_points)
    rows = [row for batch in batches for row in batch]

    for row in rows:
        wkb_hex = row["geom"]
        geom = shapely_wkb.loads(wkb_hex, hex=True)
        # Points get promoted to MultiPoint
        assert geom.geom_type == "MultiPoint"


def test_custom_geometry_column_name(three_points):
    batches, _ = parse_shapefile(three_points, geometry_column="the_geom")
    rows = [row for batch in batches for row in batch]

    assert "the_geom" in rows[0]
    assert "geom" not in rows[0]


# --- Batching ---


def test_batching_yields_correct_number_of_batches(three_points):
    batches, _ = parse_shapefile(three_points, batch_size=2)
    batch_list = list(batches)

    assert len(batch_list) == 2
    assert len(batch_list[0]) == 2
    assert len(batch_list[1]) == 1


# --- Column casing ---


def test_columns_are_lowercased_by_default(three_points):
    batches, _ = parse_shapefile(three_points)
    rows = [row for batch in batches for row in batch]

    assert "name" in rows[0]
    assert "NAME" not in rows[0]


def test_columns_preserve_case_when_lowercase_disabled(three_points):
    batches, _ = parse_shapefile(three_points, lowercase_columns=False)
    rows = [row for batch in batches for row in batch]

    assert "NAME" in rows[0]
    assert "name" not in rows[0]


# --- Geometry promotion ---


def test_point_promoted_to_multipoint(three_points):
    batches, _ = parse_shapefile(three_points)
    rows = [row for batch in batches for row in batch]

    geom = shapely_wkb.loads(rows[0]["geom"], hex=True)
    assert geom.geom_type == "MultiPoint"


def test_linestring_promoted_to_multilinestring(tmp_shapefile):
    features = [
        ({"ID": 1}, LineString([(0, 0), (1, 1)])),
    ]
    schema = {"geometry": "LineString", "properties": {"ID": "int"}}
    path = tmp_shapefile(features, schema=schema)

    batches, _ = parse_shapefile(path)
    rows = [row for batch in batches for row in batch]

    geom = shapely_wkb.loads(rows[0]["geom"], hex=True)
    assert geom.geom_type == "MultiLineString"


def test_polygon_promoted_to_multipolygon(tmp_shapefile):
    features = [
        ({"ID": 1}, Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])),
    ]
    schema = {"geometry": "Polygon", "properties": {"ID": "int"}}
    path = tmp_shapefile(features, schema=schema)

    batches, _ = parse_shapefile(path)
    rows = [row for batch in batches for row in batch]

    geom = shapely_wkb.loads(rows[0]["geom"], hex=True)
    assert geom.geom_type == "MultiPolygon"


# --- Failure handling ---


def test_features_without_geometry_are_skipped_and_recorded(tmp_path):
    """
    Write a shapefile normally, then manually corrupt one feature's geometry
    by creating a file where fiona will yield a None geometry.
    We simulate this by testing the parser's reaction to a feature
    that fails geometry conversion.
    """
    # Easiest way: write a valid shapefile, then test via a geojson-like
    # approach. Instead, let's just verify the failure path works by
    # checking that parse_shapefile handles a well-formed file cleanly.
    #
    # For a true "no geometry" test, we write a GeoJSON file that fiona
    # can read, with a null geometry feature.
    geojson_path = tmp_path / "test.geojson"
    geojson_path.write_text(
        """{
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": "good"},
                "geometry": {"type": "Point", "coordinates": [0, 0]}
            },
            {
                "type": "Feature",
                "properties": {"name": "bad"},
                "geometry": null
            },
            {
                "type": "Feature",
                "properties": {"name": "also_good"},
                "geometry": {"type": "Point", "coordinates": [1, 1]}
            }
        ]
    }"""
    )

    batches, result = parse_shapefile(geojson_path)
    rows = [row for batch in batches for row in batch]

    assert result.features_parsed == 2
    assert result.features_failed == 1
    assert result.failures[0]["index"] == 1
    assert len(rows) == 2


# --- Zip support ---


def test_reads_shapefile_from_zip(three_points, tmp_path):
    zip_path = tmp_path / "archive.zip"
    # Collect all shapefile component files (.shp, .shx, .dbf, .prj, etc.)
    shp_dir = three_points.parent
    shp_stem = three_points.stem

    with zipfile.ZipFile(zip_path, "w") as zf:
        for f in shp_dir.glob(f"{shp_stem}.*"):
            zf.write(f, f.name)

    batches, result = parse_shapefile(zip_path)
    rows = [row for batch in batches for row in batch]

    assert result.features_parsed == 3
    assert len(rows) == 3


# --- ParseResult tracking ---


def test_parse_result_tracks_source_crs(three_points):
    batches, result = parse_shapefile(three_points)
    # Must consume the iterator for result to be populated
    list(batches)

    # fiona writes a default CRS for ESRI Shapefiles
    assert result.source_crs is not None
