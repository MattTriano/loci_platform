"""
Abstract base class for all dataset specs.

Every data source (Census, TIGER, OSM, Socrata) defines a concrete
DatasetSpec subclass that describes *what* to collect and *where* to
store it.  This base class captures the fields and interface that are
common across all sources.
"""

from __future__ import annotations

from abc import ABC


class DatasetSpec(ABC):
    """
    Common interface for dataset specifications.

    Subclasses are expected to be dataclasses that declare at least:
        name: str
        target_table: str
        target_schema: str
        entity_key: list[str] | None

    entity_key may be a plain field, set in __post_init__, or a
    property — as long as it's accessible as spec.entity_key.
    """

    name: str
    target_table: str
    target_schema: str
    entity_key: list[str] | None
