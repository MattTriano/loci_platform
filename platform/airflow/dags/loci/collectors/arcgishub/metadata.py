# /loci_platform/platform/airflow/dags/loci/collectors/arcgishub/metadata.py
"""Catalog/metadata access for ArcGIS Hub data portals."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from loci.collectors.arcgishub.client import ArcGISHubClient


class ArcGISHubMetadata:
    """
    Browses the dataset catalog of an ArcGIS Hub site.

    Wraps the OGC API - Records endpoint exposed by every Hub site at
    `/api/search/v1/collections/{collection}/items`. Returns the raw
    item dicts the API provides -- no parsing or normalization.

    Usage:
        client = ArcGISHubClient("https://data.tps.ca")
        meta = ArcGISHubMetadata(client)

        for item in meta.search(q="crime", limit=50):
            print(item["id"], item["properties"]["title"])

        details = meta.get_dataset("abc123...")
    """

    def __init__(
        self,
        client: ArcGISHubClient,
        collection: str = "dataset",
    ) -> None:
        self._client = client
        self._collection = collection
        self._items_path = f"/api/search/v1/collections/{collection}/items"

    def search(self, **params: Any) -> Iterator[dict[str, Any]]:
        """
        Yield matching catalog items, paginating automatically.

        Any keyword arguments are passed through as query parameters. Useful
        ones include `q` (free-text search), `limit` (page size), and
        `filter` (CQL expression). See the Hub OGC Records docs for the
        full set.
        """
        yield from self._client.paginate(self._items_path, params=params or None)

    # @cached_property
    @property
    def all_datasets(self) -> list[dict[str, Any]]:
        """Return all catalog items as a list. Convenience wrapper around search()."""
        return list(self.search(limit=100))

    def list_datasets(self) -> list[dict[str, Any]]:
        return self.all_datasets

    def get_dataset(self, item_id: str) -> dict[str, Any]:
        """
        Return the full metadata dict for a single catalog item.
        """
        return self._client.get_json(f"{self._items_path}/{item_id}")
