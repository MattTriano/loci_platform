from __future__ import annotations

from unittest.mock import patch

import pytest
from loci.collectors.tiger.spec import (
    ALL_STATE_FIPS,
    TigerDatasetSpec,
    _default_entity_key,
    _fiona_type_to_pg,
    _geom_pg_type,
    _schema_to_columns,
    generate_tiger_ddl,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tract_spec():
    """A typical state-scoped spec for census tracts."""
    return TigerDatasetSpec(
        name="census_tracts",
        layer="TRACT",
        vintages=[2023, 2024],
        target_table="census_tracts",
        target_schema="raw_data",
        source="tiger",
        state_fips=["06", "36"],
    )


@pytest.fixture
def national_spec():
    """A national-scope spec (e.g. primary roads)."""
    return TigerDatasetSpec(
        name="primary_roads",
        layer="PRIMARYROADS",
        vintages=[2023],
        target_table="primary_roads",
        target_schema="raw_data",
        source="tiger",
    )


@pytest.fixture
def sample_fiona_schema():
    """A realistic fiona schema dict for a tract shapefile."""
    return {
        "STATEFP": "str:2",
        "COUNTYFP": "str:3",
        "TRACTCE": "str:6",
        "GEOID": "str:11",
        "NAME": "str:7",
        "ALAND": "int:14",
        "AWATER": "int:14",
        "INTPTLAT": "str:11",
        "INTPTLON": "str:12",
    }


@pytest.fixture
def addr_fiona_schema():
    """A fiona schema dict for an ADDR shapefile (no geometry, no GEOID)."""
    return {
        "TLID": "int:10",
        "FROMHN": "str:12",
        "TOHN": "str:12",
        "SIDE": "str:1",
        "ZIP": "str:5",
        "PLUS4": "str:4",
        "FROMTYP": "str:1",
        "TOTYP": "str:1",
        "ARID": "str:22",
        "MTFCC": "str:5",
    }


@pytest.fixture
def coastline_fiona_schema():
    """A fiona schema dict for a COASTLINE shapefile (no GEOID)."""
    return {
        "NAME": "str:100",
        "MTFCC": "str:5",
    }


# ---------------------------------------------------------------------------
# TigerDatasetSpec
# ---------------------------------------------------------------------------


class TestTigerDatasetSpec:
    def test_rejects_unknown_source(self):
        with pytest.raises(ValueError, match="Unknown source"):
            TigerDatasetSpec(
                name="bad",
                layer="TRACT",
                vintages=[2023],
                target_table="bad",
                source="shapefile",
            )

    def test_accepts_tiger_source(self):
        spec = TigerDatasetSpec(
            name="t", layer="TRACT", vintages=[2023], target_table="t", source="tiger"
        )
        assert spec.source == "tiger"

    def test_accepts_cartographic_source(self):
        spec = TigerDatasetSpec(
            name="t", layer="TRACT", vintages=[2023], target_table="t", source="cartographic"
        )
        assert spec.source == "cartographic"

    def test_states_returns_explicit_fips(self, tract_spec):
        assert tract_spec.states == ["06", "36"]

    def test_states_defaults_to_all_states(self, national_spec):
        assert national_spec.states == ALL_STATE_FIPS

    @patch(
        "loci.collectors.tiger.spec.TIGER_LAYER_SCOPE",
        {"PRIMARYROADS": "national", "TRACT": "state"},
    )
    def test_scope_uses_layer_lookup(self):
        spec = TigerDatasetSpec(
            name="roads", layer="PRIMARYROADS", vintages=[2023], target_table="roads"
        )
        assert spec.scope == "national"

    @patch("loci.collectors.tiger.spec.TIGER_LAYER_SCOPE", {"PRIMARYROADS": "national"})
    def test_scope_defaults_to_state(self):
        spec = TigerDatasetSpec(
            name="tracts", layer="TRACT", vintages=[2023], target_table="tracts"
        )
        assert spec.scope == "state"

    @patch("loci.collectors.tiger.spec.TIGER_LAYER_SCOPE", {"TRACT": "state"})
    def test_scope_is_case_insensitive(self):
        spec = TigerDatasetSpec(
            name="tracts", layer="tract", vintages=[2023], target_table="tracts"
        )
        assert spec.scope == "state"


# ---------------------------------------------------------------------------
# Schema / type conversion
# ---------------------------------------------------------------------------


class TestSchemaToColumns:
    def test_converts_fiona_schema_to_pg_columns(self, sample_fiona_schema):
        columns = _schema_to_columns(sample_fiona_schema, "Polygon", "geom", 4326, lowercase=True)
        col_dict = dict(columns)
        assert col_dict["geoid"] == "text"
        assert col_dict["aland"] == "bigint"

    def test_lowercases_column_names_when_requested(self, sample_fiona_schema):
        columns = _schema_to_columns(sample_fiona_schema, "Polygon", "geom", 4326, lowercase=True)
        names = [name for name, _ in columns]
        assert all(n == n.lower() for n in names)

    def test_preserves_case_when_lowercase_is_false(self, sample_fiona_schema):
        columns = _schema_to_columns(sample_fiona_schema, "Polygon", "geom", 4326, lowercase=False)
        names = [name for name, _ in columns]
        assert "GEOID" in names
        assert "STATEFP" in names


class TestFionaTypeToPg:
    @pytest.mark.parametrize(
        "fiona_type, expected",
        [
            ("str:80", "text"),
            ("str", "text"),
            ("int:14", "bigint"),
            ("int", "bigint"),
            ("float:24.15", "double precision"),
            ("date", "date"),
            ("datetime", "timestamptz"),
        ],
    )
    def test_maps_known_types(self, fiona_type, expected):
        assert _fiona_type_to_pg(fiona_type) == expected

    def test_unknown_type_defaults_to_text(self):
        assert _fiona_type_to_pg("exotic:99") == "text"


class TestGeomPgType:
    @pytest.mark.parametrize(
        "geom_type",
        ["Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon"],
    )
    def test_promotes_to_multi_variant(self, geom_type):
        result = _geom_pg_type(geom_type, 4326)
        assert "Multi" in result

    def test_uses_requested_srid(self):
        result = _geom_pg_type("Polygon", 3857)
        assert "3857" in result
        assert "4326" not in result

    def test_unknown_geom_type_produces_generic_fallback(self):
        result = _geom_pg_type("GeometryCollection", 4326)
        assert result == "geometry(Geometry, 4326)"

    def test_none_geom_type_produces_generic_fallback(self):
        result = _geom_pg_type(None, 4326)
        assert result == "geometry(Geometry, 4326)"


# ---------------------------------------------------------------------------
# _default_entity_key
# ---------------------------------------------------------------------------


class TestDefaultEntityKey:
    def test_detects_geoid(self, sample_fiona_schema):
        result = _default_entity_key(sample_fiona_schema, lowercase=True)
        assert result == ["geoid", "vintage"]

    def test_detects_year_suffixed_geoid(self):
        schema = {"GEOID20": "str:11", "CLASSFP20": "str:2"}
        result = _default_entity_key(schema, lowercase=True)
        assert result == ["geoid20", "vintage"]

    def test_detects_geoid_uppercase(self, sample_fiona_schema):
        result = _default_entity_key(sample_fiona_schema, lowercase=False)
        assert result == ["GEOID", "vintage"]

    def test_detects_tlid_when_no_geoid(self, addr_fiona_schema):
        result = _default_entity_key(addr_fiona_schema, lowercase=True)
        assert result == ["tlid", "vintage"]

    def test_returns_none_when_no_candidates(self, coastline_fiona_schema):
        result = _default_entity_key(coastline_fiona_schema, lowercase=True)
        assert result is None

    def test_prefers_geoid_over_tlid(self):
        schema = {"GEOID": "str:11", "TLID": "int:10"}
        result = _default_entity_key(schema, lowercase=True)
        assert result == ["geoid", "vintage"]

    def test_detects_linearid(self):
        schema = {"LINEARID": "str:22", "NAME": "str:100"}
        result = _default_entity_key(schema, lowercase=True)
        assert result == ["linearid", "vintage"]


# ---------------------------------------------------------------------------
# generate_tiger_ddl
# ---------------------------------------------------------------------------


class TestGenerateTigerDDL:
    """Test DDL generation with mocked I/O (no network, no fiona)."""

    def _mock_inspect(self, schema, geom_type="Polygon"):
        """Patch _inspect_sample_schema to return a canned schema."""
        return patch(
            "loci.collectors.tiger.spec._inspect_sample_schema",
            return_value=(schema, geom_type),
        )

    def test_creates_table_in_correct_schema(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec)
        assert "create table raw_data.census_tracts" in ddl

    def test_includes_vintage_column(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec)
        assert '"vintage" integer not null' in ddl

    def test_includes_scd2_columns(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec)
        assert '"record_hash" text not null' in ddl
        assert '"valid_from" timestamptz' in ddl
        assert '"valid_to" timestamptz' in ddl
        assert '"ingested_at" timestamptz' in ddl

    def test_includes_geometry_column(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec)
        assert '"geom" geometry(MultiPolygon, 4326)' in ddl

    def test_includes_unique_constraint_on_entity_key(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec)
        assert "uq_census_tracts_entity_hash" in ddl
        assert '"geoid", "vintage", "record_hash"' in ddl

    def test_includes_current_version_index(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec)
        assert "ix_census_tracts_current" in ddl
        assert '"valid_to" is null' in ddl

    def test_includes_spatial_index(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec)
        assert "ix_census_tracts_geom" in ddl
        assert "using gist" in ddl

    def test_uses_most_recent_vintage_by_default(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema) as mock:
            generate_tiger_ddl(tract_spec)
        mock.assert_called_once_with(tract_spec, 2024)

    def test_uses_explicit_vintage(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema) as mock:
            generate_tiger_ddl(tract_spec, vintage=2023)
        mock.assert_called_once_with(tract_spec, 2023)

    def test_custom_entity_key(self, sample_fiona_schema):
        spec = TigerDatasetSpec(
            name="roads",
            layer="ROADS",
            vintages=[2023],
            target_table="roads",
            entity_key=["linearid", "vintage"],
        )
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(spec)
        assert '"linearid", "vintage", "record_hash"' in ddl

    def test_custom_geometry_column_name(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec, geometry_column="the_geom")
        assert '"the_geom"' in ddl
        assert "ix_census_tracts_the_geom" in ddl

    def test_lowercases_shapefile_columns(self, tract_spec, sample_fiona_schema):
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(tract_spec)
        assert '"statefp"' in ddl
        assert '"STATEFP"' not in ddl

    # -- No entity key / no SCD2 --

    def test_omits_scd2_columns_when_no_entity_key(self, coastline_fiona_schema):
        spec = TigerDatasetSpec(
            name="coastline",
            layer="COASTLINE",
            vintages=[2024],
            target_table="coastline",
        )
        with self._mock_inspect(coastline_fiona_schema, geom_type="LineString"):
            ddl = generate_tiger_ddl(spec)
        assert '"ingested_at" timestamptz' in ddl
        assert '"record_hash"' not in ddl
        assert '"valid_from"' not in ddl
        assert '"valid_to"' not in ddl

    def test_omits_unique_constraint_when_no_entity_key(self, coastline_fiona_schema):
        spec = TigerDatasetSpec(
            name="coastline",
            layer="COASTLINE",
            vintages=[2024],
            target_table="coastline",
        )
        with self._mock_inspect(coastline_fiona_schema, geom_type="LineString"):
            ddl = generate_tiger_ddl(spec)
        assert "uq_" not in ddl
        assert "ix_coastline_current" not in ddl

    def test_still_includes_spatial_index_when_no_entity_key(self, coastline_fiona_schema):
        spec = TigerDatasetSpec(
            name="coastline",
            layer="COASTLINE",
            vintages=[2024],
            target_table="coastline",
        )
        with self._mock_inspect(coastline_fiona_schema, geom_type="LineString"):
            ddl = generate_tiger_ddl(spec)
        assert "ix_coastline_geom" in ddl
        assert "using gist" in ddl

    # -- No geometry --

    def test_omits_geometry_column_when_geom_is_none_string(self, addr_fiona_schema):
        spec = TigerDatasetSpec(
            name="addrs",
            layer="ADDR",
            vintages=[2024],
            target_table="addrs",
        )
        with self._mock_inspect(addr_fiona_schema, geom_type="None"):
            ddl = generate_tiger_ddl(spec)
        assert '"geom"' not in ddl

    def test_omits_spatial_index_when_no_geometry(self, addr_fiona_schema):
        spec = TigerDatasetSpec(
            name="addrs",
            layer="ADDR",
            vintages=[2024],
            target_table="addrs",
        )
        with self._mock_inspect(addr_fiona_schema, geom_type="None"):
            ddl = generate_tiger_ddl(spec)
        assert "using gist" not in ddl

    # -- Synthetic FIPS columns for county-scoped layers --

    @patch(
        "loci.collectors.tiger.spec.TIGER_LAYER_SCOPE",
        {"ADDR": "county"},
    )
    def test_adds_synthetic_fips_when_missing_from_schema(self, addr_fiona_schema):
        spec = TigerDatasetSpec(
            name="addrs",
            layer="ADDR",
            vintages=[2024],
            target_table="addrs",
        )
        with self._mock_inspect(addr_fiona_schema, geom_type="None"):
            ddl = generate_tiger_ddl(spec)
        assert '"statefp" text' in ddl
        assert '"countyfp" text' in ddl

    @patch(
        "loci.collectors.tiger.spec.TIGER_LAYER_SCOPE",
        {"ROADS": "county"},
    )
    def test_does_not_add_synthetic_fips_when_already_in_schema(self):
        schema = {
            "STATEFP": "str:2",
            "COUNTYFP": "str:3",
            "LINEARID": "str:22",
            "FULLNAME": "str:100",
        }
        spec = TigerDatasetSpec(
            name="roads",
            layer="ROADS",
            vintages=[2024],
            target_table="roads",
        )
        with self._mock_inspect(schema, geom_type="LineString"):
            ddl = generate_tiger_ddl(spec)
        # statefp should appear exactly once (from the schema, not synthetic)
        assert ddl.count('"statefp" text') == 1
        assert ddl.count('"countyfp" text') == 1

    def test_does_not_add_synthetic_fips_for_state_scoped_layers(self, sample_fiona_schema):
        spec = TigerDatasetSpec(
            name="tracts",
            layer="TRACT",
            vintages=[2024],
            target_table="tracts",
        )
        with self._mock_inspect(sample_fiona_schema):
            ddl = generate_tiger_ddl(spec)
        # statefp comes from the schema, not synthetic — count should be 1
        assert ddl.count('"statefp"') == 1
