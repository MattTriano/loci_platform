from __future__ import annotations

from dataclasses import dataclass


@dataclass
class IncrementalConfig:
    """
    Configuration for incremental loading of a specific dataset.

    Attributes:
        incremental_column:  The Socrata column to filter on (e.g. "updated_on").
                             Should be monotonically increasing for new/changed rows.
        conflict_key:        Column(s) forming the natural key for upsert.
                             e.g. ["case_number"] or ["pin14", "tax_year"].
        columns:             Optional subset of columns to SELECT. None = all.
        order_by:            Explicit $order clause. Defaults to incremental_column.
                             Use "updated_on, :id" if the column has duplicates.
        where:               Additional static $where filter (combined via AND).
    """

    incremental_column: str
    conflict_key: list[str]
    columns: list[str] | None = None
    order_by: str | None = None
    where: str | None = None


@dataclass
class SCD2Config:
    """
    Configuration for SCD2 (slowly changing dimension type 2) table generation.

    When provided to generate_ddl, adds record_hash, valid_from, and valid_to
    columns, a unique constraint on (entity_key, record_hash), and an index
    for efficient current-version queries (WHERE valid_to IS NULL).

    Attributes:
        entity_key: Column(s) forming the natural key that identifies an entity
                    across versions. e.g. ["sr_number"] or ["pin14", "tax_year"].
    """

    entity_key: list[str]
