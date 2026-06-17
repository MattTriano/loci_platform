# /loci_platform/platform/airflow/dags/loci/collectors/cms/spec.py
"""
CMSDatasetSpec — defines a data.cms.gov dataset to collect.

Usage:
    from loci.collectors.cms.spec import CMSDatasetSpec

    spec = CMSDatasetSpec(
        name="medicare_inpatient_by_provider_and_service",
        dataset_title="Medicare Inpatient Hospitals - by Provider and Service",
        target_table="medicare_inpatient_by_provider_and_service",
        entity_key=["rndrng_prvdr_ccn", "drg_cd", "vintage"],
        retrieval="api",
    )
"""

from __future__ import annotations

from dataclasses import dataclass

from loci.collectors.base_spec import DatasetSpec


@dataclass
class CMSDatasetSpec(DatasetSpec):
    """
    Defines a data.cms.gov dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "medicare_inpatient_by_provider_and_service").
    dataset_title : str
        Exact dataset title as it appears in the data.json catalog
        (e.g. "Medicare Inpatient Hospitals - by Provider and Service").
        Use CMSMetadata.titles() to find it.
    target_table : str
        Destination table name. Keep it under ~48 characters so the
        generated constraint/index names stay within Postgres's
        63-character identifier limit.
    target_schema : str
        Destination schema name. Default "raw_data".
    entity_key : list[str]
        Columns that uniquely identify a record for SCD2 merge, using
        the lowercased column names. Must include "vintage" — each
        published version is a distinct slice of the entity space, and
        omitting it would make rows from different years collide as
        "changed" versions of one entity.
    retrieval : str
        "api" to page rows out of the versioned JSON API (fine up to a
        few hundred thousand rows per vintage), "csv" to download the
        version's CSV distribution (use for the multimillion-row
        datasets). Default "api".
    vintages : list[str] | None
        Vintage labels to collect (e.g. ["2022", "2023", "2024"]), as
        produced by CMSMetadata.versions(). None means all published
        versions. Default None.
    """

    name: str
    dataset_title: str
    target_table: str
    target_schema: str = "raw_data"
    entity_key: list[str] | None = None
    retrieval: str = "api"
    vintages: list[str] | None = None
    source: str = "cms"

    def __post_init__(self):
        if self.retrieval not in ("api", "csv"):
            raise ValueError(f"retrieval must be 'api' or 'csv', got {self.retrieval!r}")
        if not self.entity_key:
            raise ValueError("entity_key is required (CMS collection is SCD2-only).")
        if "vintage" not in self.entity_key:
            raise ValueError(
                "entity_key must include 'vintage' — without it, rows from "
                "different published versions collide as one entity."
            )

    @property
    def dataset_id(self) -> str:
        return self.target_table
