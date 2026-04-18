"""
ArcGISHubDatasetSpec — defines an ArcGIS Hub dataset to collect.

Usage:
    from loci.collectors.arcgis_hub.spec import ArcGISHubDatasetSpec

    spec = ArcGISHubDatasetSpec(
        name="tps_arrests",
        item_id="4702e79fd2404f7d93dd9866f45d7ec2",
        target_table="tps_arrests",
        entity_key=["Event_Unique_Id"],
    )
"""

from __future__ import annotations

from dataclasses import dataclass

from loci.collectors.base_spec import DatasetSpec


@dataclass
class ArcGISHubDatasetSpec(DatasetSpec):
    """
    Defines an ArcGIS Hub dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "tps_arrests").
    item_id : str
        Hub catalog item id (e.g. "4702e79fd2404f7d93dd9866f45d7ec2").
    target_table : str
        Destination table name.
    target_schema : str
        Destination schema name. Default "raw_data".
    entity_key : list[str] | None
        Columns that uniquely identify a record for SCD2 merge.
        IMPORTANT: do not use OBJECTID -- it is not stable across
        service refreshes. Use a domain field (e.g. a case/event id).
        None means append-only ingestion.
    layer_index : int
        Which layer of the Feature Service to pull. Default 0.
        A single Hub item can wrap a service with multiple layers.
    where : str
        Server-side SQL filter applied to every request. Default "1=1".
    incremental_column : str | None
        Date/time field used for incremental updates (e.g.
        "last_edited_date"). None means full refresh on every run.
    out_fields : list[str] | None
        Subset of fields to request. None means all fields.
    source : str
        Source identifier. Default "arcgis_hub".
    """

    name: str
    item_id: str
    target_table: str
    target_schema: str = "raw_data"
    entity_key: list[str] | None = None
    layer_index: int = 0
    where: str = "1=1"
    incremental_column: str | None = None
    out_fields: list[str] | None = None
    source: str = "arcgis_hub"

    @property
    def dataset_id(self) -> str:
        return f"{self.item_id}:{self.layer_index}"
