from __future__ import annotations

import pytest
from loci.collectors.config import SCD2Config
from loci.collectors.socrata.metadata import SocrataColumnInfo, SocrataTableMetadata

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_metadata(
    views_columns: list[dict] | None = None,
    catalog_resource: dict | None = None,
    dataset_name: str = "Test Dataset",
) -> SocrataTableMetadata:
    """
    Build a SocrataTableMetadata with pre-populated API responses
    so tests don't hit the network.
    """
    meta = SocrataTableMetadata.__new__(SocrataTableMetadata)
    meta.dataset_id = "abcd-1234"
    meta.logger = __import__("logging").getLogger("test")

    meta._catalog_metadata = {
        "metadata": {"domain": "data.example.org"},
        "resource": {
            "name": dataset_name,
            **(catalog_resource or {}),
        },
    }

    meta._views_metadata = {"columns": views_columns or []}
    meta._columns = None

    return meta


def _make_views_column(
    name: str,
    field_name: str,
    datatype: str = "text",
    description: str = "",
    flags: list[str] | None = None,
) -> dict:
    """Build a dict mimicking a single column from the Socrata views API."""
    col = {
        "name": name,
        "fieldName": field_name,
        "dataTypeName": datatype,
        "description": description,
    }
    if flags:
        col["flags"] = flags
    return col


# ---------------------------------------------------------------------------
# Column parsing
# ---------------------------------------------------------------------------


class TestColumnParsing:
    def test_parses_columns_from_views_api(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("Case Number", "case_number"),
                _make_views_column("Date", "date", datatype="calendar_date"),
            ]
        )

        assert len(meta.columns) == 2
        assert meta.columns[0].field_name == "case_number"
        assert meta.columns[1].field_name == "date"

    def test_excludes_hidden_columns(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("Visible", "visible"),
                _make_views_column("Hidden", "hidden", flags=["hidden"]),
            ]
        )

        field_names = [c.field_name for c in meta.columns]
        assert "visible" in field_names
        assert "hidden" not in field_names

    def test_excludes_computed_region_columns(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("Real", "real_col"),
                _make_views_column("Computed", ":@computed_region_abc123", datatype="number"),
            ]
        )

        field_names = [c.field_name for c in meta.columns]
        assert "real_col" in field_names
        assert ":@computed_region_abc123" not in field_names

    def test_falls_back_to_catalog_api_when_views_empty(self):
        meta = _make_metadata(
            views_columns=[],
            catalog_resource={
                "columns_name": ["Case Number", "Status"],
                "columns_field_name": ["case_number", "status"],
                "columns_datatype": ["Text", "Text"],
                "columns_description": ["The case ID", ""],
            },
        )

        assert len(meta.columns) == 2
        assert meta.columns[0].field_name == "case_number"
        assert meta.columns[0].description == "The case ID"

    def test_raises_when_no_columns_in_either_api(self):
        meta = _make_metadata(views_columns=[], catalog_resource={})

        with pytest.raises(ValueError, match="No column metadata found"):
            _ = meta.columns


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


class TestTypeMapping:
    @pytest.mark.parametrize(
        "socrata_type, expected_pg_type",
        [
            ("text", "text"),
            ("number", "numeric"),
            ("calendar_date", "timestamptz"),
            ("floating_timestamp", "timestamp"),
            ("checkbox", "boolean"),
            ("point", "geometry(Point, 4326)"),
            ("multipolygon", "geometry(MultiPolygon, 4326)"),
            ("money", "numeric(19,4)"),
        ],
    )
    def test_known_types_map_correctly(self, socrata_type, expected_pg_type):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("col", "col", datatype=socrata_type),
            ]
        )

        assert meta.columns[0].pg_type == expected_pg_type

    def test_unknown_type_defaults_to_text(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("col", "col", datatype="some_future_type"),
            ]
        )

        assert meta.columns[0].pg_type == "text"


# ---------------------------------------------------------------------------
# Column rename map
# ---------------------------------------------------------------------------


class TestColumnRenameMap:
    def test_includes_columns_where_name_differs_from_field_name(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("Case Number", "case_number"),
                _make_views_column("status", "status"),
            ]
        )

        rename_map = meta.column_rename_map
        assert rename_map == {"Case Number": "case_number"}

    def test_empty_when_all_names_match(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("status", "status"),
                _make_views_column("date", "date"),
            ]
        )

        assert meta.column_rename_map == {}


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------


class TestGenerateDdl:
    def test_basic_ddl_includes_data_and_system_columns(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("ID", "id", datatype="number"),
                _make_views_column("Name", "name", datatype="text"),
            ]
        )

        ddl = meta.generate_ddl(schema="public", table_name="test_table", include_comments=False)

        assert "create table if not exists public.test_table" in ddl
        assert '"id" numeric' in ddl
        assert '"name" text' in ddl
        assert '"socrata_id" text' in ddl
        assert '"socrata_updated_at" timestamptz' in ddl
        assert '"ingested_at" timestamptz' in ddl

    def test_ingested_at_excluded_when_disabled(self):
        meta = _make_metadata(
            views_columns=[_make_views_column("ID", "id")],
        )

        ddl = meta.generate_ddl(
            schema="public",
            table_name="t",
            include_ingested_at=False,
            include_comments=False,
        )

        assert "ingested_at" not in ddl

    def test_comments_generated_for_described_columns(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("ID", "id", description="Primary key"),
                _make_views_column("Name", "name", description=""),
            ]
        )

        ddl = meta.generate_ddl(schema="public", table_name="t")

        assert "comment on column public.t.\"id\" is 'Primary key'" in ddl
        assert '"name"' not in ddl.split("comment", 1)[-1]

    def test_comments_escape_single_quotes(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("Note", "note", description="It's a note"),
            ]
        )

        ddl = meta.generate_ddl(schema="public", table_name="t")

        assert "It''s a note" in ddl

    def test_scd2_config_adds_tracking_columns_and_constraints(self):
        meta = _make_metadata(
            views_columns=[
                _make_views_column("ID", "id", datatype="number"),
            ]
        )
        scd2 = SCD2Config(entity_key=["id"])

        ddl = meta.generate_ddl(
            schema="raw", table_name="crimes", scd2_config=scd2, include_comments=False
        )

        assert '"record_hash" text not null' in ddl
        assert '"valid_from" timestamptz' in ddl
        assert '"valid_to" timestamptz' in ddl
        assert "unique (id, record_hash)" in ddl
        assert "where valid_to is null" in ddl

    def test_default_table_name_used_when_none_provided(self):
        meta = _make_metadata(
            dataset_name="Chicago Police - Crimes 2024",
            views_columns=[_make_views_column("ID", "id")],
        )

        ddl = meta.generate_ddl(schema="raw", table_name=None, include_comments=False)

        assert "raw.chicago_police_crimes_2024" in ddl


# ---------------------------------------------------------------------------
# Default table name sanitization
# ---------------------------------------------------------------------------


class TestDefaultTableName:
    @pytest.mark.parametrize(
        "dataset_name, expected",
        [
            ("Crimes - 2024", "crimes_2024"),
            ("  Leading & Trailing Spaces  ", "leading_trailing_spaces"),
            ("UPPER CASE", "upper_case"),
            ("already_clean", "already_clean"),
            ("lots!!!of***special###chars", "lots_of_special_chars"),
        ],
    )
    def test_sanitizes_name(self, dataset_name, expected):
        meta = _make_metadata(dataset_name=dataset_name, views_columns=[])
        assert meta._default_table_name() == expected


# ---------------------------------------------------------------------------
# SocrataColumnInfo.ddl_fragment
# ---------------------------------------------------------------------------


class TestColumnInfoDdlFragment:
    def test_produces_quoted_column_definition(self):
        col = SocrataColumnInfo(
            name="Case Number",
            field_name="case_number",
            datatype="text",
            description="",
            pg_type="text",
        )

        assert col.ddl_fragment == '    "case_number" text'
