# /loci_platform/platform/airflow/dags/loci/collectors/arcgishub/spec.py
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
    base_url : str
        The base URL for the data source.
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
    layer_index : int | list[int] | str
        Which layer(s) of the Feature Service to pull. Default 0.
        A single Hub item can wrap a service with multiple layers.
        Pass a list of ints to collect specific layers, or "all" to
        auto-discover and collect every layer.
    where : str
        Server-side SQL filter applied to every request. Default "1=1".
    incremental_column : str | None
        Date/time field used for incremental updates (e.g.
        "last_edited_date"). None means full refresh on every run.
    out_fields : list[str] | None
        Subset of fields to request. None means all fields.
    source : str
        Source identifier. Default "arcgis_hub".
    layer_column : str | None
        If set, adds a column with this name to each row containing
        the layer's name (e.g. "Traffic Crashes 2024"). None means
        no layer column is added.
    min_field_overlap : float
        When collecting multiple layers, the minimum fraction of fields
        that must be shared across all layers. Raises ValueError if
        overlap is below this threshold. Default 0.8.
    """

    name: str
    base_url: str
    item_id: str
    target_table: str
    target_schema: str = "raw_data"
    entity_key: list[str] | None = None
    layer_index: int | list[int] | str = 0
    where: str = "1=1"
    incremental_column: str | None = None
    out_fields: list[str] | None = None
    source: str = "arcgis_hub"
    layer_column: str | None = None
    min_field_overlap: float = 0.8

    @property
    def dataset_id(self) -> str:
        if isinstance(self.layer_index, int):
            return f"{self.item_id}:{self.layer_index}"
        return f"{self.item_id}:multi"

    @property
    def is_multi_layer(self) -> bool:
        return isinstance(self.layer_index, list) or self.layer_index == "all"
