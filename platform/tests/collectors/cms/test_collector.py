# /loci_platform/platform/tests/collectors/cms/test_collector.py
"""
Critical observable behaviors of CMSCollector, tested end to end against
a real Postgres (real StagedIngest SCD2 semantics) with a fake CMS API.

Each test names one behavior the tooling must keep exhibiting. The
assertions look only at the target table and the collect() summary, so
the implementation underneath is free to change.
"""

from __future__ import annotations

import dataclasses

from .helpers import DATASET_TITLE, make_rows, seeded_source

TABLE = "fake_payments"


def current_counts(engine, schema) -> dict[str, int]:
    df = engine.query(
        f'select "vintage", count(*) as n from {schema}.{TABLE} '
        f'where "valid_to" is null group by "vintage"'
    )
    return {row["vintage"]: int(row["n"]) for _, row in df.iterrows()}


def total_count(engine, schema) -> int:
    df = engine.query(f"select count(*) as n from {schema}.{TABLE}")
    return int(df["n"].iloc[0])


# ---------------------------------------------------------------------
# Behavior 1: a first collect ingests every published vintage in full,
# with normalized column names and per-row vintage/source-date stamps.
# ---------------------------------------------------------------------


def test_first_collect_ingests_every_vintage_completely(engine, schema, warehouse):
    collector, spec = warehouse(seeded_source())

    summary = collector.collect(spec)

    assert summary["errors"] == []
    assert summary["versions_processed"] == 2
    assert current_counts(engine, schema) == {"2022": 7, "2023": 7}

    df = engine.query(
        f"select rndrng_prvdr_ccn, avg_payment_amt, vintage, _source_modified "
        f"from {schema}.{TABLE} where vintage = '2022' limit 1"
    )
    assert df["_source_modified"].iloc[0] == "2023-05-10"
    # Columns are queryable unquoted (lowercased, space -> underscore):
    # the select above would have raised otherwise.


# ---------------------------------------------------------------------
# Behavior 2: collecting again with an unchanged source does nothing.
# ---------------------------------------------------------------------


def test_repeat_collect_skips_everything(engine, schema, warehouse):
    collector, spec = warehouse(seeded_source())
    collector.collect(spec)

    summary = collector.collect(spec)

    assert summary["versions_processed"] == 0
    assert summary["versions_skipped"] == 2
    assert summary["total_rows_staged"] == 0
    assert total_count(engine, schema) == 14


# ---------------------------------------------------------------------
# Behavior 3: recollection (force or otherwise) never duplicates data.
# ---------------------------------------------------------------------


def test_force_recollect_creates_no_duplicates(engine, schema, warehouse):
    collector, spec = warehouse(seeded_source())
    collector.collect(spec)

    summary = collector.collect(spec, force=True)

    assert summary["versions_processed"] == 2
    assert summary["total_rows_merged"] == 0
    assert total_count(engine, schema) == 14


# ---------------------------------------------------------------------
# Behavior 4: a re-release with changed content recollects only that
# vintage, versions exactly the changed rows, and then resettles —
# the following collect skips everything again.
# ---------------------------------------------------------------------


def test_changed_rerelease_versions_only_what_changed_then_resettles(engine, schema, warehouse):
    source = seeded_source()
    collector, spec = warehouse(source)
    collector.collect(spec)

    corrected = make_rows(n=7)
    corrected[0]["Avg Payment Amt"] = "150.0"
    source.set_version(DATASET_TITLE, "2022", corrected, modified="2025-01-01")

    summary = collector.collect(spec)
    assert summary["versions_processed"] == 1
    assert summary["versions_skipped"] == 1
    assert summary["total_rows_merged"] == 1

    df = engine.query(
        f"select avg_payment_amt, valid_to from {schema}.{TABLE} "
        f"where vintage = '2022' and rndrng_prvdr_ccn = '000000'"
    )
    assert len(df) == 2  # old version closed, new version current
    current = df[df["valid_to"].isnull()]
    assert len(current) == 1
    assert current["avg_payment_amt"].iloc[0] == "150.0"
    assert current_counts(engine, schema)["2022"] == 7

    resettled = collector.collect(spec)
    assert resettled["versions_skipped"] == 2


# ---------------------------------------------------------------------
# Behavior 5: a re-release with IDENTICAL content is recollected once
# and then resettles — it must not be re-downloaded on every run.
# ---------------------------------------------------------------------


def test_unchanged_rerelease_resettles_after_one_pass(engine, schema, warehouse):
    source = seeded_source()
    collector, spec = warehouse(source)
    collector.collect(spec)

    source.set_version(DATASET_TITLE, "2022", make_rows(n=7), modified="2025-01-01")

    second = collector.collect(spec)
    assert second["versions_processed"] == 1
    assert second["total_rows_merged"] == 0  # everything deduped

    third = collector.collect(spec)
    assert third["versions_skipped"] == 2
    assert third["versions_processed"] == 0


# ---------------------------------------------------------------------
# Behavior 6: a vintage with fewer current rows than the source (a
# partial/failed ingest) is detected and healed.
# ---------------------------------------------------------------------


def test_incomplete_vintage_is_detected_and_healed(engine, schema, warehouse):
    collector, spec = warehouse(seeded_source())
    collector.collect(spec)

    engine.execute(
        f"delete from {schema}.{TABLE} "
        f"where vintage = '2022' and rndrng_prvdr_ccn in ('000000', '000001', '000002')"
    )
    assert current_counts(engine, schema)["2022"] == 4

    summary = collector.collect(spec)

    assert summary["versions_processed"] == 1
    assert summary["versions_skipped"] == 1
    assert current_counts(engine, schema)["2022"] == 7


# ---------------------------------------------------------------------
# Behavior 7: when a re-release REMOVES rows, the vintage is recollected
# once and then resettles — a warehouse count above the source count
# must not loop. (Pins the documented limitation too: removed rows stay
# current. If per-vintage invalidation is ever added, update the last
# assertion deliberately.)
# ---------------------------------------------------------------------


def test_row_removal_in_rerelease_does_not_cause_recollect_loop(engine, schema, warehouse):
    source = seeded_source()
    collector, spec = warehouse(source)
    collector.collect(spec)

    source.set_version(DATASET_TITLE, "2022", make_rows(n=7)[1:], modified="2025-01-01")

    second = collector.collect(spec)
    assert second["versions_processed"] == 1

    third = collector.collect(spec)
    assert third["versions_skipped"] == 2  # 7 current >= 6 in source: fresh

    df = engine.query(
        f"select valid_to from {schema}.{TABLE} "
        f"where vintage = '2022' and rndrng_prvdr_ccn = '000000'"
    )
    assert df["valid_to"].isnull().all()  # removed row remains current (v1 limitation)


# ---------------------------------------------------------------------
# Behavior 8: spec.vintages restricts collection to the requested ones.
# ---------------------------------------------------------------------


def test_vintages_filter_limits_scope(engine, schema, warehouse):
    collector, spec = warehouse(seeded_source(), vintages=["2023"])

    summary = collector.collect(spec)

    assert summary["versions_processed"] == 1
    assert current_counts(engine, schema) == {"2023": 7}


# ---------------------------------------------------------------------
# Behavior 9: a failing vintage doesn't block the others; the failure
# is reported in the summary.
# ---------------------------------------------------------------------


def test_failure_in_one_vintage_does_not_block_others(engine, schema, warehouse):
    source = seeded_source()
    collector, spec = warehouse(source)
    collector.client.fail_uuids.add(source.uuid_for(DATASET_TITLE, "2022"))

    summary = collector.collect(spec)

    assert summary["versions_processed"] == 1
    assert len(summary["errors"]) == 1
    assert summary["errors"][0]["vintage"] == "2022"
    assert current_counts(engine, schema) == {"2023": 7}


# ---------------------------------------------------------------------
# Behavior 10: collecting into a missing target table fails per-vintage
# with reported errors, not an unhandled crash.
# ---------------------------------------------------------------------


def test_collect_without_target_table_reports_errors_not_crash(engine, schema, warehouse):
    collector, spec = warehouse(seeded_source(), create_table=False)

    summary = collector.collect(spec)

    assert summary["versions_processed"] == 0
    assert len(summary["errors"]) == 2


# ---------------------------------------------------------------------
# Behavior 11: the API and CSV retrieval paths are version-equivalent —
# recollecting via the other path creates zero spurious SCD2 versions,
# including for empty-string values (API "") vs NULL (CSV parse).
# ---------------------------------------------------------------------


def test_csv_and_api_retrieval_are_version_equivalent(engine, schema, warehouse):
    source = seeded_source()  # 2023 includes an empty-string payment value
    collector, spec = warehouse(source, retrieval="api")
    collector.collect(spec)

    csv_spec = dataclasses.replace(spec, retrieval="csv")
    summary = collector.collect(csv_spec, force=True)

    assert summary["versions_processed"] == 2
    assert summary["total_rows_merged"] == 0
    assert total_count(engine, schema) == 14
