# /loci_platform/platform/airflow/dags/loci/collectors/ckan/metadata.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)


@dataclass
class CKANResource:
    """Lightweight wrapper around a CKAN resource dict."""

    id: str
    name: str | None
    format: str | None
    url: str | None
    size: int | None
    last_modified: str | None
    created: str | None
    datastore_active: bool

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CKANResource:
        return cls(
            id=d["id"],
            name=d.get("name"),
            format=(d.get("format") or "").upper() or None,
            url=d.get("url"),
            size=d.get("size"),
            last_modified=d.get("last_modified"),
            created=d.get("created"),
            datastore_active=d.get("datastore_active", False),
        )


class CKANMetadata:
    """Client for exploring and introspecting a CKAN data portal."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._api_base: str | None = None
        self._session = requests.Session()
        self.logger = logging.getLogger("ckan_metadata")

    @property
    def api_base(self) -> str:
        """Resolve and cache the API base path (v3 preferred, v2 fallback)."""
        if self._api_base is None:
            self._api_base = self._detect_api_base()
        return self._api_base

    def _detect_api_base(self) -> str:
        """Probe the portal to find a working API path.

        Tries /api/3/action first (CKAN 2.2+), then /api/action (common alias),
        then /api/2/rest as a last resort for very old portals.
        """
        candidates = [
            "/api/3/action",
            "/api/action",
        ]
        for path in candidates:
            try:
                url = f"{self.base_url}{path}/status_show"
                resp = self._session.get(url, timeout=15)
                if resp.status_code == 200 and resp.json().get("success"):
                    self.logger.info("Using API base: %s", path)
                    return f"{self.base_url}{path}"
            except Exception:
                continue

        # Default to v3 — most portals support it even if status_show
        # isn't available without auth.
        self.logger.warning(
            "Could not probe API version for %s, defaulting to /api/3/action",
            self.base_url,
        )
        return f"{self.base_url}/api/3/action"

    def _get(self, action: str, params: dict | None = None) -> dict:
        """Make a GET request to a CKAN action endpoint and return the result."""
        url = f"{self.api_base}/{action}"
        resp = self._session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise ValueError(f"CKAN API error on {action}: {data.get('error', data)}")
        return data

    # ------------------------------------------------------------------
    # Portal exploration
    # ------------------------------------------------------------------

    def portal_version(self) -> str:
        """Return the CKAN version string for this portal."""
        data = self._get("status_show")
        return data["result"]["ckan_version"]

    def list_datasets(self) -> list[str]:
        """Return all dataset names (package IDs) on this portal."""
        data = self._get("package_list")
        return data["result"]

    def get_dataset_metadata(self, dataset_id: str) -> dict:
        """Return full metadata for a dataset including its resources.

        dataset_id can be the package name (URL slug) or UUID.
        """
        data = self._get("package_show", params={"id": dataset_id})
        return data["result"]

    def search_datasets(self, query: str, rows: int = 10) -> list[dict]:
        """Search for datasets matching a query string.

        Uses CKAN's Solr-backed search, so standard Solr query syntax works.
        """
        data = self._get("package_search", params={"q": query, "rows": rows})
        return data["result"]["results"]

    def list_organizations(self) -> list[str]:
        """Return all organization names on this portal."""
        data = self._get("organization_list")
        return data["result"]

    def list_groups(self) -> list[str]:
        """Return all group names on this portal."""
        data = self._get("group_list")
        return data["result"]

    def list_tags(self) -> list[str]:
        """Return all tags used across datasets on this portal."""
        data = self._get("tag_list")
        return data["result"]

    def get_organization(self, org_id: str) -> dict:
        """Return details for an organization, including its datasets."""
        data = self._get(
            "organization_show",
            params={"id": org_id, "include_datasets": True},
        )
        return data["result"]

    def get_group(self, group_id: str) -> dict:
        """Return details for a group, including its datasets."""
        data = self._get(
            "group_show",
            params={"id": group_id, "include_datasets": True},
        )
        return data["result"]

    def get_tag(self, tag_id: str) -> dict:
        """Return details for a tag, including datasets that use it."""
        data = self._get("tag_show", params={"id": tag_id})
        return data["result"]

    # ------------------------------------------------------------------
    # Resource helpers
    # ------------------------------------------------------------------

    def get_resources(self, dataset_id: str) -> list[CKANResource]:
        """Return all resources for a dataset as CKANResource objects."""
        meta = self.get_dataset_metadata(dataset_id)
        return [CKANResource.from_dict(r) for r in meta.get("resources", [])]

    def get_resource(self, resource_id: str) -> CKANResource:
        """Return a single resource by its UUID."""
        data = self._get("resource_show", params={"id": resource_id})
        return CKANResource.from_dict(data["result"])

    def find_resources(self, dataset_id: str, fmt: str) -> list[CKANResource]:
        """Return resources from a dataset that match a given format (case-insensitive)."""
        return [r for r in self.get_resources(dataset_id) if r.format == fmt.upper()]

    def dataset_last_modified(self, dataset_id: str) -> str | None:
        """Return the metadata_modified timestamp for a dataset, or None."""
        meta = self.get_dataset_metadata(dataset_id)
        return meta.get("metadata_modified")

    # ------------------------------------------------------------------
    # DataStore detection
    # ------------------------------------------------------------------

    def has_datastore(self, resource_id: str) -> bool:
        """Check whether a resource has DataStore data available.

        Tries a minimal datastore_search request. If the DataStore extension
        is not installed or the resource isn't in the DataStore, this returns False.
        """
        try:
            self._get(
                "datastore_search",
                params={"resource_id": resource_id, "limit": "0"},
            )
            return True
        except Exception:
            return False

    def get_datastore_fields(self, resource_id: str) -> list[dict[str, str]]:
        """Return the field definitions from the DataStore for a resource.

        Each field is a dict with at least 'id' (column name) and 'type'
        (e.g. 'text', 'int', 'numeric', 'timestamp').

        Raises ValueError if the resource is not in the DataStore.
        """
        data = self._get("datastore_search", params={"resource_id": resource_id, "limit": "0"})
        fields = data["result"].get("fields", [])
        # Filter CKAN's internal columns: _id (auto-increment) and _full_text (tsvector).
        internal = {"_id", "_full_text"}
        return [f for f in fields if f.get("id") not in internal]

    def get_datastore_row_count(self, resource_id: str) -> int:
        """Return the total number of rows in a DataStore resource."""
        data = self._get(
            "datastore_search",
            params={"resource_id": resource_id, "limit": "0", "include_total": "true"},
        )
        return data["result"].get("total", 0)
