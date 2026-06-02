# /loci_platform/platform/airflow/dags/loci/collectors/ckan/spec.py
"""
CKANDatasetSpec — defines a CKAN dataset to collect.

Usage:
    from loci.collectors.ckan.spec import CKANDatasetSpec

    spec = CKANDatasetSpec(
        name="chicago_food_inspections",
        base_url="https://data.cityofchicago.org",
        dataset_id="4ijn-s7e5",
        target_table="chicago_food_inspections",
        entity_key=["inspection_id"],
        resource_format="CSV",
    )

    # Or target specific resources by ID:
    spec = CKANDatasetSpec(
        name="transit_stops",
        base_url="https://data.gov",
        dataset_id="transit-stops-2024",
        target_table="transit_stops",
        resource_ids=["a1b2c3d4-...", "e5f6g7h8-..."],
    )
"""

from __future__ import annotations

from dataclasses import dataclass

from loci.collectors.base_spec import DatasetSpec


@dataclass
class CKANDatasetSpec(DatasetSpec):
    """
    Defines a CKAN dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "chicago_food_inspections").
    base_url : str
        Root URL of the CKAN portal (e.g. "https://data.cityofchicago.org").
    dataset_id : str
        CKAN package name (URL slug) or UUID.
    target_table : str
        Destination table name.
    target_schema : str
        Destination schema name. Default "raw_data".
    entity_key : list[str] | None
        Columns that uniquely identify a record for SCD2 merge.
        None means append-only ingestion.
    resource_ids : list[str] | None
        Explicit list of CKAN resource UUIDs to ingest. If provided,
        only these resources are downloaded. Takes precedence over
        resource_format.
    resource_format : str | None
        Format filter (e.g. "CSV", "GeoJSON"). All resources matching
        this format are ingested. Ignored if resource_ids is set.
    """

    name: str
    base_url: str
    dataset_id: str
    target_table: str
    target_schema: str = "raw_data"
    entity_key: list[str] | None = None
    resource_ids: list[str] | None = None
    resource_format: str | None = None
    source: str = "ckan"

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")
        if not self.resource_ids and not self.resource_format:
            raise ValueError(
                "Provide either resource_ids (explicit resource UUIDs) or "
                "resource_format (e.g. 'CSV') to select which resources to ingest."
            )
