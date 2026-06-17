# /loci_platform/platform/tests/collectors/dkan/helpers.py
"""
Fakes and test-data helpers for the DKAN collector tests.

The boundary that gets faked is HTTP (FakeDKANClient duck-types
DKANClient); ingestion in the DB-backed tests runs against a real
Postgres so StagedIngest's actual SCD2 semantics are part of the
behavior under test.

Fidelity note: real DKAN serves NORMALIZED column names from the
datastore (lowercase, whitespace -> underscore, other punctuation
dropped) while distribution files keep the ORIGINAL headers. The fake
reproduces that asymmetry — rows are stored with raw headers, the
datastore path normalizes them, the file path writes them raw — because
the collector's contract is that the two retrieval paths converge.
(That DKAN's real rule matches was verified by the live smoke test.)
"""

from __future__ import annotations

import csv
import re
import tempfile
from pathlib import Path

from loci.collectors.dkan.spec import DKANDatasetSpec

PDC_BASE_URL = "https://fake-pdc.cms.gov"
HOSPITAL_ID = "xubh-q36u"
OP_BASE_URL = "https://fake-openpayments.cms.gov"
OP_2023_ID = "op-2023-uuid"
OP_2024_ID = "op-2024-uuid"

# A raw header that normalizes to 79 chars — past Postgres's 63 limit.
LONG_RAW_HEADER = "Extremely Long Measure Column Name That Exceeds The Postgres Identifier Limit"


def dkan_normalize(raw: str) -> str:
    """The external system's normalization, as observed in the smoke test."""
    name = raw.strip().lower()
    name = re.sub(r"\s+", "_", name)
    return re.sub(r"[^a-z0-9_]", "", name)


# ---------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------


class FakeDKANSource:
    """In-memory stand-in for one DKAN portal: identifier -> dataset."""

    def __init__(self):
        # identifier -> {"title", "modified", "rows" (raw-header dicts)}
        self.datasets: dict[str, dict] = {}

    def set_dataset(self, identifier: str, title: str, rows: list[dict], modified: str) -> None:
        """Publish or re-publish (refresh in place) one dataset."""
        self.datasets[identifier] = {"title": title, "modified": modified, "rows": rows}

    def _entry(self, identifier: str) -> dict:
        ds = self.datasets[identifier]
        return {
            "identifier": identifier,
            "title": ds["title"],
            "modified": ds["modified"],
            "distribution": [
                {
                    "title": f"{ds['title']} (csv)",
                    "mediaType": "text/csv",
                    "downloadURL": f"https://fake-files.cms.gov/{identifier}.csv",
                }
            ],
        }


class FakeDKANClient:
    """Duck-types DKANClient against a FakeDKANSource. No HTTP."""

    def __init__(self, base_url: str, source: FakeDKANSource, page_size: int = 3):
        self.base_url = base_url.rstrip("/")
        self.source = source
        self.page_size = page_size  # small default so paging is exercised
        self.fail_identifiers: set[str] = set()

    def _rows_normalized(self, identifier: str) -> list[dict]:
        if identifier in self.fail_identifiers:
            raise RuntimeError(f"Injected failure for {identifier}")
        rows = self.source.datasets[identifier]["rows"]
        return [{dkan_normalize(k): v for k, v in row.items()} for row in rows]

    # -- metastore --

    def get_catalog(self, refresh: bool = False) -> list[dict]:
        return [self.source._entry(i) for i in self.source.datasets]

    def get_dataset(self, identifier: str) -> dict:
        return self.source._entry(identifier)

    # -- datastore (serves normalized keys, as real DKAN does) --

    def row_count(self, identifier: str, index: int = 0) -> int:
        return len(self._rows_normalized(identifier))

    def get_page(self, identifier: str, size: int, offset: int, index: int = 0) -> list[dict]:
        return [dict(r) for r in self._rows_normalized(identifier)[offset : offset + size]]

    def iter_pages(self, identifier: str, size: int | None = None, index: int = 0):
        size = size or self.page_size
        offset = 0
        while True:
            page = self.get_page(identifier, size=size, offset=offset, index=index)
            if not page:
                return
            yield page
            if len(page) < size:
                return
            offset += size

    # -- files (writes RAW headers, as real distribution files have) --

    def download_to_tempfile(self, url: str, suffix: str = ".csv") -> Path:
        for identifier, ds in self.source.datasets.items():
            if identifier in url:
                if identifier in self.fail_identifiers:
                    raise RuntimeError(f"Injected failure for {url}")
                tmp = tempfile.NamedTemporaryFile(
                    mode="w", suffix=suffix, prefix="fake_dkan_", delete=False, newline=""
                )
                writer = csv.DictWriter(tmp, fieldnames=list(ds["rows"][0].keys()))
                writer.writeheader()
                writer.writerows(ds["rows"])
                tmp.close()
                return Path(tmp.name)
        raise KeyError(f"No fake file at {url}")


# ---------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------


def make_hospital_rows(n: int = 7, rating: str = "3", blank_rating_index: int | None = 1):
    """
    Raw-header rows shaped like the PDC source: a slash header (DKAN
    drops the slash), a long header (forces 63-char truncation), and an
    empty-string value (exercises ''/NULL hash equivalence between the
    datastore and file paths).
    """
    rows = []
    for i in range(n):
        rows.append(
            {
                "Facility ID": f"{i:06d}",
                "County/Parish": "HOUSTON",
                "Hospital Rating": "" if i == blank_rating_index else rating,
                LONG_RAW_HEADER: f"m{i}",
            }
        )
    return rows


def make_payment_rows(year: str, n: int = 5, amount: str = "10.5"):
    return [
        {
            "Record ID": f"R{i:04d}",
            "Program Year": year,
            "Total Amount of Payment USDollars": amount,
        }
        for i in range(n)
    ]


def seeded_pdc_source() -> FakeDKANSource:
    source = FakeDKANSource()
    source.set_dataset(
        HOSPITAL_ID, "Hospital General Information", make_hospital_rows(), modified="2026-04-28"
    )
    return source


def seeded_op_source() -> FakeDKANSource:
    source = FakeDKANSource()
    source.set_dataset(
        OP_2023_ID, "2023 General Payment Data", make_payment_rows("2023"), modified="2026-01-27"
    )
    source.set_dataset(
        OP_2024_ID, "2024 General Payment Data", make_payment_rows("2024"), modified="2026-01-27"
    )
    return source


def make_hospital_spec(schema: str, retrieval: str = "datastore", **overrides) -> DKANDatasetSpec:
    kwargs = dict(
        name="pdc_hospital_general_information",
        base_url=PDC_BASE_URL,
        dataset_identifiers=[HOSPITAL_ID],
        target_table="fake_hospitals",
        target_schema=schema,
        entity_key=["facility_id"],
        retrieval=retrieval,
    )
    kwargs.update(overrides)
    return DKANDatasetSpec(**kwargs)


def make_payments_spec(schema: str, retrieval: str = "datastore", **overrides) -> DKANDatasetSpec:
    kwargs = dict(
        name="openpayments_general_payments",
        base_url=OP_BASE_URL,
        dataset_identifiers=[OP_2023_ID, OP_2024_ID],
        target_table="fake_payments",
        target_schema=schema,
        entity_key=["record_id", "program_year"],
        retrieval=retrieval,
    )
    kwargs.update(overrides)
    return DKANDatasetSpec(**kwargs)
