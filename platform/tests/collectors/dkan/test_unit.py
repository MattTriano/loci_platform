# /loci_platform/platform/tests/collectors/dkan/test_unit.py
"""
Unit behaviors of the DKAN tooling that need neither a database nor
HTTP: spec validation, catalog/distribution parsing, and the DDL
contract.
"""

from __future__ import annotations

import pytest
from loci.collectors.dkan.collector import PG_MAX_IDENTIFIER, DKANCollector
from loci.collectors.dkan.metadata import DKANMetadata
from loci.collectors.dkan.spec import DKANDatasetSpec

from ..common import NoopTracker
from .helpers import (
    LONG_RAW_HEADER,
    FakeDKANClient,
    dkan_normalize,
    make_hospital_spec,
    make_payments_spec,
    seeded_op_source,
    seeded_pdc_source,
)

# ---------------------------------------------------------------------
# Behavior: specs that can't be executed safely fail loudly at
# construction time.
# ---------------------------------------------------------------------


def test_spec_requires_dataset_identifiers():
    with pytest.raises(ValueError, match="dataset_identifiers"):
        DKANDatasetSpec(name="x", base_url="https://p", target_table="x", entity_key=["id"])


def test_spec_requires_entity_key():
    with pytest.raises(ValueError, match="entity_key"):
        DKANDatasetSpec(name="x", base_url="https://p", dataset_identifiers=["a"], target_table="x")


def test_spec_rejects_unknown_retrieval_mode():
    with pytest.raises(ValueError, match="retrieval"):
        make_hospital_spec("raw_data", retrieval="carrier_pigeon")


def test_spec_forbids_invalidate_missing_for_multi_dataset_families():
    """Invalidation with a family staged one sibling at a time would
    close out every other sibling's rows — must fail at construction."""
    with pytest.raises(ValueError, match="invalidate_missing"):
        make_payments_spec("raw_data", invalidate_missing=True)


def test_spec_allows_invalidate_missing_for_single_dataset():
    spec = make_hospital_spec("raw_data", invalidate_missing=True)
    assert spec.invalidate_missing is True


# ---------------------------------------------------------------------
# Behavior: distribution parsing tolerates both catalog shapes (flat
# and nested under "data" in reference-id form).
# ---------------------------------------------------------------------


def test_distributions_parse_flat_and_nested_shapes():
    dataset = {
        "distribution": [
            {"mediaType": "text/csv", "downloadURL": "https://x/a.csv"},
            {"data": {"mediaType": "text/csv", "downloadURL": "https://x/b.csv"}},
        ]
    }
    dists = DKANMetadata.distributions(dataset)
    assert [d.index for d in dists] == [0, 1]
    assert [d.download_url for d in dists] == ["https://x/a.csv", "https://x/b.csv"]


# ---------------------------------------------------------------------
# Behavior: generated DDL defines a table the collector can ingest
# into directly — DKAN-rule column names, 63-char truncation, the
# cross-sibling union for families, provenance columns, and SCD2
# uniqueness on (entity_key, record_hash).
# ---------------------------------------------------------------------


def _ddl_collector(spec, source):
    return DKANCollector(
        engine=object(),
        tracker=NoopTracker(),
        clients={spec.base_url: FakeDKANClient(spec.base_url, source)},
    )


def test_ddl_uses_dkan_normalization_and_truncates_long_names():
    spec = make_hospital_spec("raw_data")
    ddl = _ddl_collector(spec, seeded_pdc_source()).generate_ddl(spec)

    # DKAN's rule drops the slash rather than underscoring it.
    assert '"countyparish" text' in ddl
    assert "county_parish" not in ddl

    long_normalized = dkan_normalize(LONG_RAW_HEADER)
    assert len(long_normalized) > PG_MAX_IDENTIFIER
    truncated = long_normalized[:PG_MAX_IDENTIFIER].rstrip("_")
    assert f'"{truncated}" text' in ddl
    assert long_normalized not in ddl  # never emit a >63-char identifier

    assert '"_source_dataset" text not null' in ddl
    assert 'unique ("facility_id", "record_hash")' in ddl
    assert 'where "valid_to" is null' in ddl


def test_ddl_unions_columns_across_family_siblings():
    source = seeded_op_source()
    # Give one sibling an extra column the other lacks.
    extra = [dict(r, **{"New 2024 Column": "x"}) for r in source.datasets["op-2024-uuid"]["rows"]]
    source.set_dataset("op-2024-uuid", "2024 General Payment Data", extra, modified="2026-01-27")

    spec = make_payments_spec("raw_data")
    ddl = _ddl_collector(spec, source).generate_ddl(spec)

    for col in (
        "record_id",
        "program_year",
        "total_amount_of_payment_usdollars",
        "new_2024_column",
    ):
        assert f'"{col}" text' in ddl
