# /loci_platform/platform/tests/collectors/cms/test_unit.py
"""
Unit behaviors of the CMS tooling that need neither a database nor HTTP:
spec validation, catalog version parsing, and the DDL contract.
"""

from __future__ import annotations

import pytest
from loci.collectors.cms.collector import CMSCollector
from loci.collectors.cms.metadata import CMSMetadata, vintage_from_temporal
from loci.collectors.cms.spec import CMSDatasetSpec

from ..common import NoopTracker
from .helpers import DATASET_TITLE, FakeCMSClient, FakeCMSSource, make_rows, make_spec

# ---------------------------------------------------------------------
# Behavior: specs that would corrupt SCD2 history or can't be executed
# fail loudly at construction time.
# ---------------------------------------------------------------------


def test_spec_rejects_entity_key_without_vintage():
    with pytest.raises(ValueError, match="vintage"):
        CMSDatasetSpec(
            name="x",
            dataset_title="X",
            target_table="x",
            entity_key=["rndrng_prvdr_ccn", "drg_cd"],
        )


def test_spec_requires_entity_key():
    with pytest.raises(ValueError, match="entity_key"):
        CMSDatasetSpec(name="x", dataset_title="X", target_table="x")


def test_spec_rejects_unknown_retrieval_mode():
    with pytest.raises(ValueError, match="retrieval"):
        CMSDatasetSpec(
            name="x",
            dataset_title="X",
            target_table="x",
            entity_key=["vintage"],
            retrieval="ftp",
        )


# ---------------------------------------------------------------------
# Behavior: the catalog's distributions resolve to one version record
# per vintage, carrying both retrieval handles and the modified date,
# oldest first, with the undated "latest" duplicate excluded.
# ---------------------------------------------------------------------


def test_versions_resolve_per_vintage_with_both_handles():
    source = FakeCMSSource()
    source.set_version(DATASET_TITLE, "2023", make_rows(2), modified="2024-06-04")
    source.set_version(DATASET_TITLE, "2022", make_rows(2), modified="2023-05-10")
    meta = CMSMetadata(FakeCMSClient(source))

    versions = meta.versions(meta.get_dataset(DATASET_TITLE))

    assert [v.vintage for v in versions] == ["2022", "2023"]  # oldest first
    for v in versions:
        assert v.api_uuid == source.uuid_for(DATASET_TITLE, v.vintage)
        assert v.csv_url
        assert v.modified
    # Only the two dated vintages — the undated "latest" entry the fake
    # catalog includes (mirroring the real one) must not appear.
    assert len(versions) == 2


def test_non_calendar_year_temporal_falls_back_to_raw_label():
    assert vintage_from_temporal("2022-01-01/2022-12-31") == "2022"
    assert vintage_from_temporal("2022-07-01/2023-06-30") == "2022-07-01/2023-06-30"


# ---------------------------------------------------------------------
# Behavior: generated DDL defines a table the collector can ingest into
# directly — every column from every in-scope vintage (normalized,
# text-typed), the vintage and source-metadata columns, and SCD2
# uniqueness on (entity_key, record_hash).
# ---------------------------------------------------------------------


def test_ddl_unions_columns_across_vintages_normalized_as_text():
    source = FakeCMSSource()
    # An older vintage with a legacy column that later vintages dropped.
    legacy_rows = [
        {"Rndrng_Prvdr_CCN": "000001", "DRG_Cd": "001", "Legacy Col": "x", "Avg Payment Amt": "1"}
    ]
    source.set_version(DATASET_TITLE, "2013", legacy_rows, modified="2023-05-10")
    source.set_version(DATASET_TITLE, "2023", make_rows(2), modified="2024-06-04")

    collector = CMSCollector(engine=object(), client=FakeCMSClient(source), tracker=NoopTracker())
    ddl = collector.generate_ddl(make_spec(schema="raw_data"))

    for col in ("rndrng_prvdr_ccn", "drg_cd", "avg_payment_amt", "legacy_col"):
        assert f'"{col}" text' in ddl
    assert '"vintage" text not null' in ddl
    assert '"_source_modified"' in ddl
    assert '"record_hash"' in ddl
    assert 'unique ("rndrng_prvdr_ccn", "drg_cd", "vintage", "record_hash")' in ddl
    assert 'where "valid_to" is null' in ddl
