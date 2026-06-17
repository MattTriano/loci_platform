# /loci_platform/platform/airflow/dags/loci/collectors/dkan/spec.py
"""
DKANSpec — defines a DKAN dataset (or family of datasets) to collect.

Usage:
    from loci.collectors.dkan.spec import DKANSpec

    # Single refresh-in-place dataset (Provider Data Catalog):
    spec = DKANSpec(
        name="pdc_hospital_general_information",
        base_url="https://data.cms.gov/provider-data",
        dataset_identifiers=["xubh-q36u"],
        target_table="pdc_hospital_general_information",
        entity_key=["facility_id"],
        retrieval="datastore",
    )

    # Multi-dataset family into one table (Open Payments program years):
    spec = DKANSpec(
        name="openpayments_general_payments",
        base_url="https://openpaymentsdata.cms.gov",
        dataset_identifiers=["fb3a65aa-...", "e6b17c6a-..."],
        target_table="openpayments_general_payments",
        entity_key=["record_id", "program_year"],
        retrieval="file",
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loci.collectors.base_spec import DatasetSpec


@dataclass
class DKANDatasetSpec(DatasetSpec):
    """
    Defines a DKAN dataset (or family of datasets) to collect.

    Parameters
    ----------
    name : str
        Human-readable name.
    base_url : str
        Root URL of the DKAN portal
        (e.g. "https://data.cms.gov/provider-data").
    dataset_identifiers : list[str]
        Metastore dataset identifiers. One entry for a normal
        refresh-in-place dataset; several when a logical dataset is
        published as a family (e.g. one Open Payments dataset per
        program year), all landing in the same target table.
    target_table : str
        Destination table name. Keep it under ~48 characters so the
        generated constraint/index names stay within Postgres's
        63-character identifier limit.
    target_schema : str
        Destination schema name. Default "raw_data".
    entity_key : list[str]
        Columns that uniquely identify a record for SCD2 merge, using
        normalized column names. IMPORTANT: when dataset_identifiers
        has more than one entry, the entity key must distinguish
        entities across the datasets (e.g. include program_year for
        Open Payments); otherwise rows from sibling datasets collide
        as "changed" versions of one entity.
    retrieval : str
        "datastore" to page rows out of the datastore API (capped at
        500 rows/page — fine up to a few hundred thousand rows), or
        "file" to download each dataset's distribution file (use for
        the multimillion-row datasets). Default "datastore".
    invalidate_missing : bool
        If True, a collection closes out current rows whose entity is
        absent from the fresh pull — correct for refresh-in-place
        datasets where disappearance means delisting (e.g. a hospital
        leaving Care Compare). Only valid for single-dataset specs:
        with a multi-dataset family, staging holds one sibling at a
        time, so invalidation would close out every other sibling's
        rows. Default False, consistent with the other collectors.
    """

    name: str
    base_url: str
    dataset_identifiers: list[str] = field(default_factory=list)
    target_table: str = ""
    target_schema: str = "raw_data"
    entity_key: list[str] | None = None
    retrieval: str = "datastore"
    invalidate_missing: bool = False
    source: str = "dkan"

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")
        if not self.dataset_identifiers:
            raise ValueError("dataset_identifiers must have at least one entry.")
        if not self.target_table:
            raise ValueError("target_table is required.")
        if not self.entity_key:
            raise ValueError("entity_key is required (DKAN collection is SCD2-only).")
        if self.retrieval not in ("datastore", "file"):
            raise ValueError(f"retrieval must be 'datastore' or 'file', got {self.retrieval!r}")
        if self.invalidate_missing and len(self.dataset_identifiers) > 1:
            raise ValueError(
                "invalidate_missing is only valid for single-dataset specs: with a "
                "multi-dataset family, staging holds one sibling at a time, so "
                "invalidation would close out every other sibling's rows."
            )

    @property
    def dataset_id(self) -> str:
        return self.target_table
