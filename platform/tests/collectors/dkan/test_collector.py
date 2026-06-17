# /loci_platform/platform/tests/collectors/dkan/test_collector.py
"""
Critical observable behaviors of DKANCollector, tested end to end
against a real Postgres (real StagedIngest SCD2 semantics) with a fake
DKAN portal at the HTTP boundary.

Each test names one behavior the tooling must keep exhibiting. The
assertions look only at the target table and the collect() summary, so
the implementation underneath is free to change.
"""

from __future__ import annotations

import dataclasses

from .helpers import (
    HOSPITAL_ID,
    LONG_RAW_HEADER,
    OP_2023_ID,
    OP_2024_ID,
    dkan_normalize,
    make_hospital_rows,
    make_hospital_spec,
    make_payments_spec,
    seeded_op_source,
    seeded_pdc_source,
)

LONG_COL = dkan_normalize(LONG_RAW_HEADER)[:63].rstrip("_")


def total_count(engine, schema, table) -> int:
    df = engine.query(f"select count(*) as n from {schema}.{table}")
    return int(df["n"].iloc[0])


def current_count(engine, schema, table, where: str = "true") -> int:
    df = engine.query(
        f'select count(*) as n from {schema}.{table} where "valid_to" is null and {where}'
    )
    return int(df["n"].iloc[0])


# ---------------------------------------------------------------------
# Behavior 1: a first collect ingests the dataset in full, with
# DKAN-normalized column names (slash dropped, long name truncated to a
# valid identifier with its data intact) and provenance stamps.
# ---------------------------------------------------------------------


def test_first_collect_ingests_with_normalization_and_provenance(engine, schema, warehouse):
    spec = make_hospital_spec(schema)
    collector = warehouse(spec, seeded_pdc_source())

    summary = collector.collect(spec)

    assert summary["errors"] == []
    assert summary["datasets_processed"] == 1
    assert total_count(engine, schema, spec.target_table) == 7

    df = engine.query(
        f'select facility_id, countyparish, "{LONG_COL}", '
        f"_source_dataset, _source_modified from {schema}.{spec.target_table} limit 7"
    )
    assert (df["_source_dataset"] == HOSPITAL_ID).all()
    assert (df["_source_modified"] == "2026-04-28").all()
    assert df[LONG_COL].notnull().all()  # the 79-char header's data survived truncation


# ---------------------------------------------------------------------
# Behavior 2: collecting again with an unchanged source does nothing.
# ---------------------------------------------------------------------


def test_repeat_collect_skips_everything(engine, schema, warehouse):
    spec = make_hospital_spec(schema)
    collector = warehouse(spec, seeded_pdc_source())
    collector.collect(spec)

    summary = collector.collect(spec)

    assert summary["datasets_processed"] == 0
    assert summary["datasets_skipped"] == 1
    assert summary["total_rows_staged"] == 0


# ---------------------------------------------------------------------
# Behavior 3: recollection (force or otherwise) never duplicates data.
# ---------------------------------------------------------------------


def test_force_recollect_creates_no_duplicates(engine, schema, warehouse):
    spec = make_hospital_spec(schema)
    collector = warehouse(spec, seeded_pdc_source())
    collector.collect(spec)

    summary = collector.collect(spec, force=True)

    assert summary["datasets_processed"] == 1
    assert summary["total_rows_merged"] == 0
    assert total_count(engine, schema, spec.target_table) == 7


# ---------------------------------------------------------------------
# Behavior 4: a re-publication with changed content versions exactly
# the changed rows, then resettles to skipping.
# ---------------------------------------------------------------------


def test_changed_republication_versions_then_resettles(engine, schema, warehouse):
    spec = make_hospital_spec(schema)
    source = seeded_pdc_source()
    collector = warehouse(spec, source)
    collector.collect(spec)

    changed = make_hospital_rows()
    changed[0]["Hospital Rating"] = "5"
    source.set_dataset(HOSPITAL_ID, "Hospital General Information", changed, "2026-07-01")

    summary = collector.collect(spec)
    assert summary["datasets_processed"] == 1
    assert summary["total_rows_merged"] == 1

    df = engine.query(
        f"select hospital_rating, valid_to from {schema}.{spec.target_table} "
        f"where facility_id = '000000'"
    )
    assert len(df) == 2  # old version closed, new version current
    current = df[df["valid_to"].isnull()]
    assert current["hospital_rating"].iloc[0] == "5"

    assert collector.collect(spec)["datasets_skipped"] == 1


# ---------------------------------------------------------------------
# Behavior 5: an identical re-publication is recollected once and then
# resettles — never re-downloaded on every run.
# ---------------------------------------------------------------------


def test_identical_republication_resettles_after_one_pass(engine, schema, warehouse):
    spec = make_hospital_spec(schema)
    source = seeded_pdc_source()
    collector = warehouse(spec, source)
    collector.collect(spec)

    source.set_dataset(
        HOSPITAL_ID, "Hospital General Information", make_hospital_rows(), "2026-07-01"
    )

    second = collector.collect(spec)
    assert second["datasets_processed"] == 1
    assert second["total_rows_merged"] == 0

    third = collector.collect(spec)
    assert third["datasets_skipped"] == 1


# ---------------------------------------------------------------------
# Behavior 6: a partially-ingested dataset is detected and healed.
# ---------------------------------------------------------------------


def test_incomplete_dataset_is_detected_and_healed(engine, schema, warehouse):
    spec = make_hospital_spec(schema)
    collector = warehouse(spec, seeded_pdc_source())
    collector.collect(spec)

    engine.execute(
        f"delete from {schema}.{spec.target_table} where facility_id in ('000000', '000001')"
    )

    summary = collector.collect(spec)

    assert summary["datasets_processed"] == 1
    assert current_count(engine, schema, spec.target_table) == 7


# ---------------------------------------------------------------------
# Behavior 7: with invalidate_missing=True, an entity absent from a
# re-publication is closed out (delisting), the count is reported, and
# the system resettles.
# ---------------------------------------------------------------------


def test_invalidate_missing_closes_delisted_entities(engine, schema, warehouse):
    spec = make_hospital_spec(schema, invalidate_missing=True)
    source = seeded_pdc_source()
    collector = warehouse(spec, source)
    collector.collect(spec)

    source.set_dataset(
        HOSPITAL_ID, "Hospital General Information", make_hospital_rows()[1:], "2026-07-01"
    )

    summary = collector.collect(spec)
    assert summary["total_rows_invalidated"] == 1

    df = engine.query(
        f"select valid_to from {schema}.{spec.target_table} where facility_id = '000000'"
    )
    assert df["valid_to"].notnull().all()  # delisted facility closed out
    assert current_count(engine, schema, spec.target_table) == 6

    assert collector.collect(spec)["datasets_skipped"] == 1


# ---------------------------------------------------------------------
# Behavior 8: with invalidate_missing=False (the default), removed rows
# stay current and the freshness check does not loop.
# ---------------------------------------------------------------------


def test_without_invalidation_removed_rows_stay_current_without_loop(engine, schema, warehouse):
    spec = make_hospital_spec(schema)
    source = seeded_pdc_source()
    collector = warehouse(spec, source)
    collector.collect(spec)

    source.set_dataset(
        HOSPITAL_ID, "Hospital General Information", make_hospital_rows()[1:], "2026-07-01"
    )

    assert collector.collect(spec)["datasets_processed"] == 1
    assert collector.collect(spec)["datasets_skipped"] == 1  # 7 current >= 6 source: no loop

    df = engine.query(
        f"select valid_to from {schema}.{spec.target_table} where facility_id = '000000'"
    )
    assert df["valid_to"].isnull().all()  # removed row remains current (flag off)


# ---------------------------------------------------------------------
# Behavior 9: a multi-dataset family lands in one table, each sibling
# tracked independently, without disturbing the other's rows.
# ---------------------------------------------------------------------


def test_family_siblings_coexist_in_one_table(engine, schema, warehouse):
    spec = make_payments_spec(schema)
    collector = warehouse(spec, seeded_op_source())

    summary = collector.collect(spec)

    assert summary["datasets_processed"] == 2
    by_dataset = engine.query(
        f"select _source_dataset, count(*) as n from {schema}.{spec.target_table} "
        f"group by _source_dataset"
    )
    counts = {row["_source_dataset"]: int(row["n"]) for _, row in by_dataset.iterrows()}
    assert counts == {OP_2023_ID: 5, OP_2024_ID: 5}


# ---------------------------------------------------------------------
# Behavior 10: a failing sibling doesn't block the others; the failure
# is reported in the summary.
# ---------------------------------------------------------------------


def test_failure_in_one_sibling_does_not_block_others(engine, schema, warehouse):
    spec = make_payments_spec(schema)
    source = seeded_op_source()
    collector = warehouse(spec, source)
    collector._client_cache[spec.base_url].fail_identifiers.add(OP_2023_ID)

    summary = collector.collect(spec)

    assert summary["datasets_processed"] == 1
    assert len(summary["errors"]) == 1
    assert summary["errors"][0]["dataset_identifier"] == OP_2023_ID
    assert current_count(engine, schema, spec.target_table) == 5  # 2024 still landed


# ---------------------------------------------------------------------
# Behavior 11: the datastore and file retrieval paths are
# version-equivalent — file headers (raw, punctuated, with empty
# strings) land on the same names and hashes the datastore produced.
# ---------------------------------------------------------------------


def test_file_and_datastore_retrieval_are_version_equivalent(engine, schema, warehouse):
    spec = make_hospital_spec(schema, retrieval="datastore")
    collector = warehouse(spec, seeded_pdc_source())
    collector.collect(spec)

    file_spec = dataclasses.replace(spec, retrieval="file")
    summary = collector.collect(file_spec, force=True)

    assert summary["datasets_processed"] == 1
    assert summary["total_rows_merged"] == 0  # zero spurious SCD2 versions
    assert total_count(engine, schema, spec.target_table) == 7


# ---------------------------------------------------------------------
# Behavior 12: collecting into a missing target table fails per-dataset
# with reported errors, not an unhandled crash.
# ---------------------------------------------------------------------


def test_collect_without_target_table_reports_errors_not_crash(engine, schema, warehouse):
    spec = make_payments_spec(schema)
    collector = warehouse(spec, seeded_op_source(), create_table=False)

    summary = collector.collect(spec)

    assert summary["datasets_processed"] == 0
    assert len(summary["errors"]) == 2
