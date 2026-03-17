"""
OsmnxDatasetSpec — defines an OSMnx bike network dataset to collect.

Unlike most collectors, OSMnx produces two tables (nodes and edges),
so the spec carries both target table names.

Usage:
    from loci.collectors.osmnx.spec import OsmnxDatasetSpec

    spec = OsmnxDatasetSpec(
        name="chicagoland_bike_network",
        target_table_nodes="osmnx_bike_network_nodes",
        target_table_edges="osmnx_bike_network_edges",
        bbox=(-87.97, 41.62, -87.5, 42.05),
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loci.collectors.base_spec import DatasetSpec


@dataclass
class OsmnxDatasetSpec(DatasetSpec):
    """
    Defines an OSMnx network dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "chicagoland_bike_network").
    target_table_nodes : str
        Destination table name for graph nodes.
    target_table_edges : str
        Destination table name for graph edges.
    target_schema : str
        Destination schema name. Default "raw_data".
    bbox : tuple[float, float, float, float]
        Bounding box as (west, south, east, north) in EPSG:4326.
    network_type : str
        OSMnx network type. Default "bike".
    entity_key_nodes : list[str]
        Entity key for the nodes table. Default ["osmid"].
    entity_key_edges : list[str]
        Entity key for the edges table. Default ["u", "v", "key"].
    """

    name: str = "chicagoland_bike_network"
    target_table_nodes: str = "osmnx_bike_network_nodes"
    target_table_edges: str = "osmnx_bike_network_edges"
    target_schema: str = "raw_data"
    bbox: tuple[float, float, float, float] = (-87.97, 41.62, -87.5, 42.05)
    network_type: str = "bike"
    entity_key_nodes: list[str] = field(default_factory=lambda: ["osmid"])
    entity_key_edges: list[str] = field(default_factory=lambda: ["u", "v", "key"])
    source: str = "osmnx"

    # --- DatasetSpec interface (not directly used since we have two tables) ---
    @property
    def target_table(self) -> str:
        return self.target_table_edges

    @property
    def entity_key(self) -> list[str]:
        return self.entity_key_edges

    @property
    def dataset_id(self) -> str:
        return self.name
