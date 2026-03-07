"""
SocrataDatasetSpec — defines a Socrata dataset to collect.

Usage:
    from loci.collectors.socrata.spec import SocrataDatasetSpec

    spec = SocrataDatasetSpec(
        name="chicago_building_permits",
        dataset_id="ydr8-5enu",
        target_table="chicago_building_permits",
        entity_key=["permit_"],
    )
"""

from __future__ import annotations

from dataclasses import dataclass

from loci.collectors.base_spec import DatasetSpec


@dataclass
class SocrataDatasetSpec(DatasetSpec):
    """
    Defines a Socrata dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "chicago_building_permits").
    dataset_id : str
        Socrata 4x4 identifier (e.g. "ydr8-5enu").
    target_table : str
        Destination table name.
    target_schema : str
        Destination schema name. Default "raw_data".
    entity_key : list[str] | None
        Columns that uniquely identify a record for SCD2 merge.
        None means no entity key (append-only ingestion).
    incremental_column : str
        Socrata system field used for incremental updates.
        Default ":updated_at".
    full_update_mode : str
        "api" for paginated SODA API, "file_download" for bulk
        CSV/GeoJSON export. Default "api".
    """

    name: str
    dataset_id: str
    target_table: str
    target_schema: str = "raw_data"
    entity_key: list[str] | None = None
    incremental_column: str = ":updated_at"
    full_update_mode: str = "api"
    source: str = "socrata"

    def __post_init__(self):
        if self.full_update_mode not in ("api", "file_download"):
            raise ValueError(
                f"full_update_mode must be 'api' or 'file_download', got {self.full_update_mode!r}"
            )
