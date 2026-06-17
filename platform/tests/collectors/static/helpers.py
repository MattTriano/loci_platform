"""
Fakes and test-data helpers for the static collector suite.

FakeStaticFileClient overrides only download(), serving bytes from an
in-memory {url: bytes} mapping — so iter_rows, CSV/XLSX parsing, column
sanitization, and encoding handling all run the real code paths with no
HTTP involved.
"""

from __future__ import annotations

import io

from loci.collectors.static.client import StaticFileClient, StaticFileDownloadError
from loci.collectors.static.spec import FileRef, StaticFileDatasetSpec


class FakeStaticFileClient(StaticFileClient):
    """Serves bytes from memory; records which URLs were requested."""

    def __init__(self, files: dict[str, bytes]):
        super().__init__(delay_seconds=0)
        self.files = dict(files)
        self.downloads: list[str] = []

    def download(self, url: str) -> bytes:
        self.downloads.append(url)
        try:
            return self.files[url]
        except KeyError:
            raise StaticFileDownloadError(f"FakeStaticFileClient has no bytes for {url}")


def csv_bytes(header: list[str], rows: list[list[str]], encoding: str = "utf-8") -> bytes:
    """Render a small CSV as bytes in the given encoding."""
    lines = [",".join(header)] + [",".join(row) for row in rows]
    return ("\n".join(lines) + "\n").encode(encoding)


def xlsx_bytes(header: list[str], rows: list[list]) -> bytes:
    """Render a small single-sheet workbook as bytes (requires openpyxl)."""
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(header)
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# Default tiny source: two health systems, one with a leading-zero id,
# one with a cp1252 en dash (0x96) in its name when encoded.
DEFAULT_URL_2023 = "https://example.test/systems-2023.csv"
DEFAULT_URL_2022 = "https://example.test/systems-2022.csv"

DEFAULT_HEADER = ["sys_id", "sys_name", "beds"]
DEFAULT_ROWS_2023 = [
    ["0895", "Adena Health System", "298"],
    ["1001", "Example Health \u2013 Metro", "512"],
]
DEFAULT_ROWS_2022 = [
    ["0895", "Adena Health System", "290"],
]


def default_files() -> dict[str, bytes]:
    return {
        DEFAULT_URL_2023: csv_bytes(DEFAULT_HEADER, DEFAULT_ROWS_2023, encoding="cp1252"),
        DEFAULT_URL_2022: csv_bytes(DEFAULT_HEADER, DEFAULT_ROWS_2022, encoding="cp1252"),
    }


def make_spec(schema: str, **overrides) -> StaticFileDatasetSpec:
    """Build a spec against the test schema with sensible defaults."""
    defaults = dict(
        name="test_static_systems",
        target_table="test_static_systems",
        target_schema=schema,
        entity_key=["sys_id", "vintage"],
        files=[
            FileRef(url=DEFAULT_URL_2023, vintage="2023", encoding="cp1252"),
        ],
    )
    defaults.update(overrides)
    return StaticFileDatasetSpec(**defaults)
