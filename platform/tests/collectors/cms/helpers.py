# /loci_platform/platform/tests/collectors/cms/helpers.py
"""
Fakes and test-data helpers for the CMS collector tests.

The boundary that gets faked is HTTP (FakeCMSClient duck-types
CMSClient); ingestion in the DB-backed tests runs against a real
Postgres so StagedIngest's actual SCD2 semantics are part of the
behavior under test.
"""

from __future__ import annotations

import csv
import uuid as uuid_module
from pathlib import Path

from loci.collectors.cms.spec import CMSDatasetSpec

DATASET_TITLE = "Fake Medicare Payments - by Provider and Service"
DATA_API_BASE = "https://data.cms.gov/data-api/v1/dataset"


# ---------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------


class FakeCMSSource:
    """
    In-memory stand-in for data.cms.gov: datasets -> vintages -> rows.

    Mutate it between collect() calls to simulate CMS publishing a new
    vintage or re-releasing an existing one with a new modified date.
    """

    def __init__(self):
        # title -> {vintage: {"temporal", "modified", "rows", "uuid", "csv_url"}}
        self.datasets: dict[str, dict[str, dict]] = {}

    def set_version(self, title: str, vintage: str, rows: list[dict], modified: str) -> None:
        """Publish or re-release one vintage. The version UUID is stable."""
        versions = self.datasets.setdefault(title, {})
        existing = versions.get(vintage)
        uid = existing["uuid"] if existing else str(uuid_module.uuid4())
        versions[vintage] = {
            "temporal": f"{vintage}-01-01/{vintage}-12-31",
            "modified": modified,
            "rows": rows,
            "uuid": uid,
            "csv_url": f"https://fake.cms.gov/files/{uid}.csv",
        }

    def uuid_for(self, title: str, vintage: str) -> str:
        return self.datasets[title][vintage]["uuid"]

    def _version_by_uuid(self, uid: str) -> dict:
        for versions in self.datasets.values():
            for v in versions.values():
                if v["uuid"] == uid:
                    return v
        raise KeyError(f"No fake version with uuid {uid}")


class FakeCMSClient:
    """Duck-types CMSClient against a FakeCMSSource. No HTTP."""

    def __init__(self, source: FakeCMSSource, page_size: int = 3):
        self.source = source
        self.page_size = page_size  # small default so paging is exercised
        self.fail_uuids: set[str] = set()  # uuids whose data calls raise

    def get_catalog(self, refresh: bool = False) -> list[dict]:
        catalog = []
        for title, versions in self.source.datasets.items():
            distributions = []
            for v in versions.values():
                distributions.append(
                    {
                        "format": "API",
                        "accessURL": f"{DATA_API_BASE}/{v['uuid']}/data",
                        "temporal": v["temporal"],
                        "modified": v["modified"],
                    }
                )
                distributions.append(
                    {
                        "mediaType": "text/csv",
                        "downloadURL": v["csv_url"],
                        "temporal": v["temporal"],
                        "modified": v["modified"],
                    }
                )
            # Undated "latest" entry, as the real catalog has — the
            # tooling is expected to ignore it.
            distributions.append(
                {"format": "API", "accessURL": f"{DATA_API_BASE}/{uuid_module.uuid4()}/data"}
            )
            catalog.append(
                {
                    "title": title,
                    "modified": max(v["modified"] for v in versions.values()),
                    "distribution": distributions,
                }
            )
        return catalog

    def _rows(self, version_uuid: str) -> list[dict]:
        if version_uuid in self.fail_uuids:
            raise RuntimeError(f"Injected failure for {version_uuid}")
        return self.source._version_by_uuid(version_uuid)["rows"]

    def row_count(self, version_uuid: str) -> int:
        return len(self._rows(version_uuid))

    def get_page(self, version_uuid: str, size: int, offset: int) -> list[dict]:
        # Fresh copies, as real JSON parsing would produce.
        return [dict(r) for r in self._rows(version_uuid)[offset : offset + size]]

    def iter_pages(self, version_uuid: str, size: int | None = None):
        size = size or self.page_size
        offset = 0
        while True:
            page = self.get_page(version_uuid, size=size, offset=offset)
            if not page:
                return
            yield page
            if len(page) < size:
                return
            offset += size

    def iter_rows(self, version_uuid: str, size: int | None = None):
        for page in self.iter_pages(version_uuid, size=size):
            yield from page

    def download_csv(self, url: str, dest_path) -> Path:
        for versions in self.source.datasets.values():
            for v in versions.values():
                if v["csv_url"] == url:
                    if v["uuid"] in self.fail_uuids:
                        raise RuntimeError(f"Injected failure for {url}")
                    dest_path = Path(dest_path)
                    headers = list(v["rows"][0].keys())
                    with open(dest_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        writer.writerows(v["rows"])
                    return dest_path
        raise KeyError(f"No fake CSV at {url}")


# ---------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------


def make_rows(n: int = 7, payment: str = "100.0", blank_payment_index: int | None = None):
    """
    Rows shaped like the source: original-cased keys, one with a space
    (exercises normalization), all values strings, optional empty string
    (exercises ''/NULL hash equivalence between API and CSV paths).
    """
    rows = []
    for i in range(n):
        amt = "" if i == blank_payment_index else payment
        rows.append({"Rndrng_Prvdr_CCN": f"{i:06d}", "DRG_Cd": "001", "Avg Payment Amt": amt})
    return rows


def seeded_source() -> FakeCMSSource:
    """A dataset with two published vintages."""
    source = FakeCMSSource()
    source.set_version(DATASET_TITLE, "2022", make_rows(n=7), modified="2023-05-10")
    source.set_version(
        DATASET_TITLE, "2023", make_rows(n=7, blank_payment_index=2), modified="2024-06-04"
    )
    return source


def make_spec(
    schema: str, retrieval: str = "api", vintages: list[str] | None = None
) -> CMSDatasetSpec:
    return CMSDatasetSpec(
        name="fake_payments",
        dataset_title=DATASET_TITLE,
        target_table="fake_payments",
        target_schema=schema,
        entity_key=["rndrng_prvdr_ccn", "drg_cd", "vintage"],
        retrieval=retrieval,
        vintages=vintages,
    )
