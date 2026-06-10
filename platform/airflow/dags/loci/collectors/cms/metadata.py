# /loci_platform/platform/airflow/dags/loci/collectors/cms/metadata.py
"""
CMSMetadata — explore the datasets available on data.cms.gov.

Built on the data.json catalog. Each dataset entry has a `distribution`
array containing every published version of the data in every available
format: entries with format "API" point at a versioned API endpoint
(each version has its own UUID), and entries with mediaType "text/csv"
are direct CSV downloads. This class groups those distributions into
CMSDatasetVersion records, which is the unit the collector will ingest.

Usage:
    from loci.collectors.cms.client import CMSClient
    from loci.collectors.cms.metadata import CMSMetadata

    meta = CMSMetadata(CMSClient())
    meta.search("inpatient hospitals")
    ds = meta.get_dataset("Medicare Inpatient Hospitals - by Provider and Service")
    for v in meta.versions(ds):
        print(v.vintage, v.modified, v.api_uuid, v.csv_url)
    meta.columns(meta.versions(ds)[-1].api_uuid)
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from loci.collectors.cms.client import CMSClient

_UUID_RE = re.compile(r"/dataset/([0-9a-f-]{36})", re.IGNORECASE)


@dataclass
class CMSDatasetVersion:
    """
    One published version (vintage) of a CMS dataset.

    Parameters
    ----------
    temporal : str
        The raw temporal coverage string from the catalog,
        e.g. "2022-01-01/2022-12-31".
    vintage : str
        Short label derived from temporal, e.g. "2022".
    modified : str | None
        The distribution-level modified date. A change here means CMS
        re-released this vintage with corrections.
    api_uuid : str | None
        UUID for the versioned API endpoint, if an API distribution exists.
    csv_url : str | None
        Direct CSV download URL, if a CSV distribution exists.
    """

    temporal: str
    vintage: str
    modified: str | None = None
    api_uuid: str | None = None
    csv_url: str | None = None


def vintage_from_temporal(temporal: str) -> str:
    """
    Derive a short vintage label from a temporal coverage string.

    "2022-01-01/2022-12-31" -> "2022" (single calendar year), otherwise
    the raw temporal string is returned unchanged.
    """
    parts = temporal.split("/")
    if len(parts) == 2:
        start_year, end_year = parts[0][:4], parts[1][:4]
        if start_year == end_year:
            return start_year
    return temporal


class CMSMetadata:
    """Explore the datasets available from data.cms.gov."""

    def __init__(self, client: CMSClient | None = None):
        self.client = client or CMSClient()

    def search(self, text: str) -> list[dict]:
        """Return catalog entries whose title contains text (case-insensitive)."""
        text = text.lower()
        return [d for d in self.client.get_catalog() if text in d.get("title", "").lower()]

    def titles(self, text: str = "") -> list[str]:
        """Return matching dataset titles — handy for browsing in a notebook."""
        return sorted(d["title"] for d in self.search(text))

    def get_dataset(self, title: str) -> dict:
        """Return the catalog entry with this exact title."""
        for d in self.client.get_catalog():
            if d.get("title") == title:
                return d
        raise KeyError(f"No dataset titled {title!r} in the data.cms.gov catalog")

    def versions(self, dataset: dict) -> list[CMSDatasetVersion]:
        """
        Return the published versions of a dataset, oldest first.

        Distributions are grouped by their temporal coverage; the
        undated "latest" API distribution is excluded (it duplicates
        the newest dated version).
        """
        by_temporal: dict[str, CMSDatasetVersion] = {}
        for dist in dataset.get("distribution", []):
            temporal = dist.get("temporal")
            if not temporal:
                continue
            version = by_temporal.setdefault(
                temporal,
                CMSDatasetVersion(temporal=temporal, vintage=vintage_from_temporal(temporal)),
            )
            version.modified = version.modified or dist.get("modified")
            if dist.get("format") == "API":
                match = _UUID_RE.search(dist.get("accessURL", ""))
                if match:
                    version.api_uuid = match.group(1)
            elif dist.get("mediaType") == "text/csv":
                version.csv_url = dist.get("downloadURL")
        return sorted(by_temporal.values(), key=lambda v: v.temporal)

    def columns(self, version_uuid: str) -> list[str]:
        """Return the column names of a dataset version, from a one-row sample."""
        page = self.client.get_page(version_uuid, size=1, offset=0)
        if not page:
            return []
        return list(page[0].keys())

    def describe(self, title: str, stats: bool = False) -> None:
        """
        Print a human-readable summary of a dataset and its versions.

        With stats=True, also fetch the row count for each version
        (one API call per version, so off by default).
        """
        ds = self.get_dataset(title)
        print(ds["title"])
        print(f"  modified: {ds.get('modified')}")
        for v in self.versions(ds):
            count = ""
            if stats and v.api_uuid:
                count = f"  rows={self.client.row_count(v.api_uuid):,}"
            formats = "+".join(
                f for f, present in [("api", v.api_uuid), ("csv", v.csv_url)] if present
            )
            print(f"  {v.vintage:<24} modified={v.modified}  [{formats}]{count}")
