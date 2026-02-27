"""
Behavioral tests for the OSM metadata, spec, and collector modules.

OsmMetadata tests hit the real Geofabrik API (marked with
pytest.mark.network so they can be skipped in CI with
`pytest -m "not network"`).

OsmDatasetSpec tests are pure unit tests.

OsmCollector and generate_ddl tests use mocks for the engine,
tracker, parse_pbf, and HTTP downloads.
"""

from __future__ import annotations

import pytest
from loci.collectors.osm.spec import OsmDatasetSpec, generate_ddl

# ================================================================== #
#  OsmDatasetSpec — pure unit tests
# ================================================================== #


class TestOsmDatasetSpecValidation:
    """Spec validation on construction."""

    def test_valid_spec_creates_successfully(self):
        spec = OsmDatasetSpec(
            name="test_nodes",
            region_ids=["us/illinois"],
            element_type="nodes",
            target_table="osm_nodes",
        )
        assert spec.element_type == "nodes"

    def test_invalid_element_type_raises(self):
        with pytest.raises(ValueError, match="element_type"):
            OsmDatasetSpec(
                name="test",
                region_ids=["us/illinois"],
                element_type="buildings",
                target_table="osm_buildings",
            )

    def test_empty_region_ids_raises(self):
        with pytest.raises(ValueError, match="region_ids"):
            OsmDatasetSpec(
                name="test",
                region_ids=[],
                element_type="nodes",
                target_table="osm_nodes",
            )

    @pytest.mark.parametrize("element_type", ["nodes", "ways", "relations"])
    def test_all_valid_element_types_accepted(self, element_type):
        spec = OsmDatasetSpec(
            name="test",
            region_ids=["illinois"],
            element_type=element_type,
            target_table=f"osm_{element_type}",
        )
        assert spec.element_type == element_type


class TestOsmDatasetSpecDefaults:
    """Default values and properties."""

    def test_default_schema(self):
        spec = OsmDatasetSpec(
            name="test",
            region_ids=["us/illinois"],
            element_type="nodes",
            target_table="osm_nodes",
        )
        assert spec.target_schema == "raw_data"

    def test_default_geometry_column(self):
        spec = OsmDatasetSpec(
            name="test",
            region_ids=["us/illinois"],
            element_type="nodes",
            target_table="osm_nodes",
        )
        assert spec.geometry_column == "geom"

    def test_default_srid(self):
        spec = OsmDatasetSpec(
            name="test",
            region_ids=["us/illinois"],
            element_type="nodes",
            target_table="osm_nodes",
        )
        assert spec.srid == 4326

    def test_entity_key_is_osm_id_and_region_id(self):
        spec = OsmDatasetSpec(
            name="test",
            region_ids=["us/illinois"],
            element_type="nodes",
            target_table="osm_nodes",
        )
        assert spec.entity_key == ["osm_id", "region_id"]


# ================================================================== #
#  generate_ddl — unit tests
# ================================================================== #


class TestGenerateDdl:
    """DDL generation for different element types."""

    def _make_spec(self, element_type: str, table: str) -> OsmDatasetSpec:
        return OsmDatasetSpec(
            name=f"test_{element_type}",
            region_ids=["us/illinois"],
            element_type=element_type,
            target_table=table,
            target_schema="raw_data",
        )

    def test_nodes_ddl_has_point_geometry(self):
        ddl = generate_ddl(self._make_spec("nodes", "osm_nodes"))
        assert "geometry(Point, 4326)" in ddl

    def test_ways_ddl_has_generic_geometry(self):
        ddl = generate_ddl(self._make_spec("ways", "osm_ways"))
        assert "geometry(Geometry, 4326)" in ddl

    def test_relations_ddl_has_no_geometry(self):
        ddl = generate_ddl(self._make_spec("relations", "osm_relations"))
        assert "geometry(" not in ddl

    def test_relations_ddl_has_members_column(self):
        ddl = generate_ddl(self._make_spec("relations", "osm_relations"))
        assert '"members" jsonb' in ddl

    def test_nodes_ddl_has_no_members_column(self):
        ddl = generate_ddl(self._make_spec("nodes", "osm_nodes"))
        assert "members" not in ddl

    def test_ddl_includes_scd2_columns(self):
        ddl = generate_ddl(self._make_spec("nodes", "osm_nodes"))
        for col in ("ingested_at", "record_hash", "valid_from", "valid_to"):
            assert f'"{col}"' in ddl

    def test_ddl_includes_osm_metadata_columns(self):
        ddl = generate_ddl(self._make_spec("nodes", "osm_nodes"))
        for col in ("osm_timestamp", "osm_version", "osm_changeset"):
            assert f'"{col}"' in ddl

    def test_ddl_includes_region_id_column(self):
        ddl = generate_ddl(self._make_spec("nodes", "osm_nodes"))
        assert '"region_id" text not null' in ddl

    def test_ddl_includes_unique_constraint(self):
        ddl = generate_ddl(self._make_spec("nodes", "osm_nodes"))
        assert "uq_osm_nodes_entity_hash" in ddl
        assert '"record_hash"' in ddl

    def test_ddl_includes_current_version_index(self):
        ddl = generate_ddl(self._make_spec("nodes", "osm_nodes"))
        assert "ix_osm_nodes_current" in ddl
        assert '"valid_to" is null' in ddl

    def test_ddl_includes_spatial_index_for_nodes(self):
        ddl = generate_ddl(self._make_spec("nodes", "osm_nodes"))
        assert "using gist" in ddl

    def test_ddl_no_spatial_index_for_relations(self):
        ddl = generate_ddl(self._make_spec("relations", "osm_relations"))
        assert "using gist" not in ddl

    def test_ddl_respects_custom_srid(self):
        spec = OsmDatasetSpec(
            name="test",
            region_ids=["us/illinois"],
            element_type="nodes",
            target_table="osm_nodes",
            srid=3857,
        )
        ddl = generate_ddl(spec)
        assert "geometry(Point, 3857)" in ddl

    def test_ddl_uses_target_schema_and_table(self):
        spec = OsmDatasetSpec(
            name="test",
            region_ids=["us/illinois"],
            element_type="nodes",
            target_table="my_nodes",
            target_schema="staging",
        )
        ddl = generate_ddl(spec)
        assert "staging.my_nodes" in ddl
