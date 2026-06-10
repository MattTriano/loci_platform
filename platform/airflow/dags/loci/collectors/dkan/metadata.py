# /loci_platform/platform/airflow/dags/loci/collectors/dkan/metadata.py
"""
DKANMetadata — explore the datasets available on a DKAN portal.

DKAN datasets are refreshed in place (no version array like
data.cms.gov): a dataset has one identifier, a dataset-level modified
date, and one or more distributions (files). The datastore is addressed
by (dataset identifier, distribution index) — index 0 for the typical
single-distribution dataset.

Usage:
    from loci.collectors.dkan.client import DKANClient
    from loci.collectors.dkan.metadata import DKANMetadata

    meta = DKANMetadata(DKANClient("https://data.cms.gov/provider-data"))
    meta.titles("hospital")
    ds = meta.get_dataset_by_title("Hospital General Information")
    meta.distributions(ds)
    meta.columns(ds["identifier"])
"""

from __future__ import annotations

from dataclasses import dataclass

from loci.collectors.dkan.client import DKANClient


@dataclass
class DKANDistribution:
    """
    One distribution (file/datastore) of a DKAN dataset.

    Parameters
    ----------
    index : int
        Position in the dataset's distribution array; this is the index
        the datastore query API addresses.
    title : str | None
    media_type : str | None
    download_url : str | None
        Direct download URL for the original file, if present.
    """

    index: int
    title: str | None = None
    media_type: str | None = None
    download_url: str | None = None


class DKANMetadata:
    """Explore the datasets available on one DKAN portal."""

    def __init__(self, client: DKANClient):
        self.client = client

    # -- catalog ------------------------------------------------------------

    def search(self, text: str) -> list[dict]:
        """Return catalog entries whose title contains text (case-insensitive)."""
        text = text.lower()
        return [d for d in self.client.get_catalog() if text in (d.get("title") or "").lower()]

    def titles(self, text: str = "") -> list[str]:
        """Return matching dataset titles — handy for browsing in a notebook."""
        return sorted(d["title"] for d in self.search(text))

    def get_dataset_by_title(self, title: str) -> dict:
        """Return the catalog entry with this exact title."""
        for d in self.client.get_catalog():
            if d.get("title") == title:
                return d
        raise KeyError(f"No dataset titled {title!r} on {self.client.base_url}")

    # -- dataset details ------------------------------------------------------

    @staticmethod
    def distributions(dataset: dict) -> list[DKANDistribution]:
        """Return a dataset's distributions, in datastore-index order."""
        out = []
        for i, dist in enumerate(dataset.get("distribution", [])):
            # Some portals nest distribution properties under "data"
            # (reference-id form); be tolerant of both shapes.
            props = dist.get("data", dist) if isinstance(dist, dict) else {}
            out.append(
                DKANDistribution(
                    index=i,
                    title=props.get("title"),
                    media_type=props.get("mediaType") or props.get("format"),
                    download_url=props.get("downloadURL"),
                )
            )
        return out

    def columns(self, dataset_identifier: str, index: int = 0) -> list[str]:
        """Return raw column names for a distribution, from a one-row sample."""
        page = self.client.get_page(dataset_identifier, size=1, offset=0, index=index)
        if not page:
            return []
        return list(page[0].keys())

    def describe(self, title: str, stats: bool = False) -> None:
        """
        Print a human-readable summary of a dataset.

        With stats=True, also fetch the datastore row count for each
        distribution (one API call per distribution, so off by default).
        """
        ds = self.get_dataset_by_title(title)
        identifier = ds.get("identifier")
        print(ds.get("title"))
        print(f"  identifier: {identifier}")
        print(f"  modified:   {ds.get('modified')}")
        for dist in self.distributions(ds):
            count = ""
            if stats:
                count = f"  rows={self.client.row_count(identifier, index=dist.index):,}"
            print(f"  [{dist.index}] {dist.title or '(untitled)'} ({dist.media_type}){count}")
            if dist.download_url:
                print(f"      file: {dist.download_url}")
