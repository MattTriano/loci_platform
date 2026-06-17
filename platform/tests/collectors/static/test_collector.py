"""
StaticFileCollector behaviors.

TestDdlContract needs no database: it checks the shape of the generated
DDL against a fake client. Everything else runs against the test
Postgres via the warehouse fixture (skipped when unavailable), and
ingests through the collector's own generated DDL.
"""

from __future__ import annotations

import pytest
from loci.collectors.static.collector import StaticFileCollector
from loci.collectors.static.spec import FileRef

from ..common import NoopTracker
from .helpers import (
    DEFAULT_HEADER,
    DEFAULT_URL_2022,
    DEFAULT_URL_2023,
    FakeStaticFileClient,
    csv_bytes,
    default_files,
    make_spec,
)

# ------------------------------------------------------------------ #
#  DDL contract (no DB)
# ------------------------------------------------------------------ #


class TestDdlContract:
    def _ddl(self, entity_key):
        spec = make_spec("raw_data", entity_key=entity_key)
        collector = StaticFileCollector(engine=None, client=FakeStaticFileClient(default_files()))
        return collector.generate_ddl(spec)

    def test_scd2_apparatus_present_with_entity_key(self):
        ddl = self._ddl(entity_key=["sys_id", "vintage"])
        for fragment in (
            '"record_hash" text not null',
            '"valid_from"',
            '"valid_to" timestamptz',
            "uq_test_static_systems_entity_hash",
            "ix_test_static_systems_current",
            "where valid_to is null",
        ):
            assert fragment in ddl

    def test_append_only_ddl_has_no_scd2_apparatus(self):
        ddl = self._ddl(entity_key=None)
        for fragment in ("record_hash", "valid_from", "valid_to", "unique", "create index"):
            assert fragment not in ddl
        # but the pipeline timestamp is still there
        assert '"ingested_at" timestamptz not null' in ddl

    def test_source_columns_are_text_and_vintage_is_not_null(self):
        ddl = self._ddl(entity_key=["sys_id", "vintage"])
        assert '"sys_id" text' in ddl
        assert '"sys_name" text' in ddl
        assert '"vintage" text not null' in ddl


# ------------------------------------------------------------------ #
#  Collection behaviors (DB-backed)
# ------------------------------------------------------------------ #


def _current(engine, spec, where: str = "true"):
    return engine.query(
        f'select * from "{spec.target_schema}"."{spec.target_table}" '
        f"where valid_to is null and {where}"
    )


class TestCollect:
    def test_ingests_rows_stamped_with_vintage(self, engine, warehouse):
        collector, spec = warehouse()
        summary = collector.collect(spec)

        assert summary["files_processed"] == 1
        assert summary["rows_staged"] == 2

        df = _current(engine, spec)
        assert len(df) == 2
        assert set(df["vintage"]) == {"2023"}

    def test_leading_zeros_and_cp1252_survive_to_the_warehouse(self, engine, warehouse):
        collector, spec = warehouse()
        collector.collect(spec)

        df = _current(engine, spec, "sys_id = '0895'")
        assert len(df) == 1  # would be 0 if anything numeric-parsed the id

        df = _current(engine, spec, "sys_id = '1001'")
        assert df["sys_name"].iloc[0] == "Example Health \u2013 Metro"

    def test_multi_file_manifest_lands_all_vintages(self, engine, schema, warehouse):
        spec = make_spec(
            schema,
            files=[
                FileRef(url=DEFAULT_URL_2022, vintage="2022", encoding="cp1252"),
                FileRef(url=DEFAULT_URL_2023, vintage="2023", encoding="cp1252"),
            ],
        )
        collector, spec = warehouse(spec=spec)
        summary = collector.collect(spec)

        assert summary["files_processed"] == 2
        df = _current(engine, spec)
        assert set(df["vintage"]) == {"2022", "2023"}

    def test_missing_table_raises_with_ddl_hint(self, warehouse):
        collector, spec = warehouse(create_table=False)
        with pytest.raises(RuntimeError, match="print_ddl"):
            collector.collect(spec)

    def test_tracker_records_one_run_per_file(self, warehouse):
        tracker = NoopTracker()
        collector, spec = warehouse(tracker=tracker)
        collector.collect(spec)

        assert len(tracker.runs) == 1
        dataset_id, run = tracker.runs[0]
        assert dataset_id == f"{spec.name}/2023"
        assert run.rows_staged == 2


class TestIdempotencyAndForce:
    def test_second_collect_skips_ingested_vintages(self, warehouse):
        collector, spec = warehouse()
        collector.collect(spec)
        # Baseline, not an absolute count: the warehouse fixture's
        # generate_ddl call also downloads the file to derive columns.
        downloads_after_first = list(collector.client.downloads)

        summary = collector.collect(spec)

        assert summary["files_skipped"] == 1
        assert summary["files_processed"] == 0
        # The skip happened before any download
        assert collector.client.downloads == downloads_after_first

    def test_new_vintage_collected_while_old_skipped(self, engine, schema, warehouse):
        collector, spec = warehouse()
        collector.collect(spec)

        # Same table, manifest grown by one edition — the manifest-append workflow.
        grown = make_spec(
            schema,
            files=[
                FileRef(url=DEFAULT_URL_2023, vintage="2023", encoding="cp1252"),
                FileRef(url=DEFAULT_URL_2022, vintage="2022", encoding="cp1252"),
            ],
        )
        summary = collector.collect(grown)

        assert summary["files_skipped"] == 1
        assert summary["files_processed"] == 1
        assert set(_current(engine, spec)["vintage"]) == {"2022", "2023"}

    def test_force_recollect_of_unchanged_file_creates_no_duplicates(self, engine, warehouse):
        collector, spec = warehouse()
        collector.collect(spec)
        collector.collect(spec, force=True)

        df = _current(engine, spec)
        assert len(df) == 2  # SCD2 deduped identical rows away

    def test_force_recollect_of_revised_file_versions_the_changed_row(self, engine, warehouse):
        collector, spec = warehouse()
        collector.collect(spec)

        # Publisher revises the file in place: 0895's bed count changes.
        revised = csv_bytes(
            DEFAULT_HEADER,
            [
                ["0895", "Adena Health System", "300"],
                ["1001", "Example Health \u2013 Metro", "512"],
            ],
            encoding="cp1252",
        )
        collector.client.files[DEFAULT_URL_2023] = revised
        collector.collect(spec, force=True)

        current = _current(engine, spec, "sys_id = '0895'")
        assert len(current) == 1
        assert current["beds"].iloc[0] == "300"

        closed = engine.query(
            f'select * from "{spec.target_schema}"."{spec.target_table}" '
            "where sys_id = '0895' and valid_to is not null"
        )
        assert len(closed) == 1
        assert closed["beds"].iloc[0] == "298"
