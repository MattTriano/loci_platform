"""
StaticFileDatasetSpec — defines a dataset published as static files at fixed URLs.

"Static" means the URL is the complete interface: no query language, no
pagination, no catalog API. The publisher overwrites a file when the data
changes; everyone who requests the URL gets the same bytes. The manifest
of FileRefs on the spec *is* the metadata for this source type, which is
why there is no StaticFileMetadata class.

Usage:
    from loci.collectors.static_file.spec import FileRef, StaticFileDatasetSpec

    spec = StaticFileDatasetSpec(
        name="ahrq_chsp_hospital_linkage",
        target_table="ahrq_chsp_hospital_linkage",
        entity_key=["ccn", "vintage"],
        files=[
            FileRef(
                url="https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
                vintage="2023",
            ),
        ],
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loci.collectors.base_spec import DatasetSpec

SUPPORTED_FORMATS = ("csv", "xlsx")


@dataclass(frozen=True)
class FileRef:
    """
    One downloadable file and how to parse it.

    Parameters
    ----------
    url : str
        Where the file lives. Supports https:// URLs and, as an escape
        hatch for sources we can't fetch programmatically (e.g. WAF
        challenges), file:// URLs pointing at manually downloaded copies.
    vintage : str
        Label for the edition/reference period this file covers
        (e.g. "2023"). Written to every row as the `vintage` column.
    file_format : str
        "csv" or "xlsx".
    encoding : str
        Text encoding for CSV files. Default "utf-8-sig" so a UTF-8 BOM,
        common in files exported from Excel, never ends up inside the
        first column name.
    delimiter : str
        CSV field delimiter.
    sheet : str | int
        Worksheet name or zero-based index for XLSX files.
    skip_rows : int
        Number of rows to skip before the header row.
    """

    url: str
    vintage: str
    file_format: str = "csv"
    encoding: str = "utf-8-sig"
    delimiter: str = ","
    sheet: str | int = 0
    skip_rows: int = 0

    def __post_init__(self):
        if self.file_format not in SUPPORTED_FORMATS:
            raise ValueError(
                f"file_format must be one of {SUPPORTED_FORMATS}, got {self.file_format!r}"
            )


@dataclass
class StaticFileDatasetSpec(DatasetSpec):
    """
    Defines a static-file dataset to collect.

    All files in the manifest land in the same target table and must
    therefore share a column layout (publishers occasionally rename
    columns between editions; if that happens, the affected edition needs
    its own spec/table, and reconciliation belongs in a downstream view).

    Every value is loaded as text. This is deliberate: identifier columns
    like CCNs carry leading zeros that numeric parsing silently destroys,
    and the raw_data layer's job is to preserve what the publisher
    published. Typing belongs downstream.

    Parameters
    ----------
    name : str
        Human-readable dataset name (e.g. "ahrq_chsp_hospital_linkage").
    target_table : str
        Destination table name.
    files : list[FileRef]
        The manifest: every file that makes up this dataset. Adding a new
        edition later is data entry, not code.
    target_schema : str
        Destination schema name. Default "raw_data".
    entity_key : list[str] | None
        Columns that uniquely identify a record for SCD2 merge. For
        edition-snapshot sources this should normally include "vintage",
        since the same hospital in two editions is two observations, not
        an update to one entity. None means append-only ingestion.
    """

    name: str
    target_table: str
    files: list[FileRef] = field(default_factory=list)
    target_schema: str = "raw_data"
    entity_key: list[str] | None = None

    def __post_init__(self):
        if not self.files:
            raise ValueError(f"spec {self.name!r} has an empty file manifest")
        vintages = [f.vintage for f in self.files]
        if len(vintages) != len(set(vintages)):
            raise ValueError(
                f"spec {self.name!r} has duplicate vintages in its manifest; "
                "each FileRef needs a distinct vintage label"
            )

    @property
    def dataset_id(self) -> str:
        return self.target_table
