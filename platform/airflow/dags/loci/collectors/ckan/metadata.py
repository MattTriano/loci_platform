import requests


class CKANMetadata:
    """A simple client for interacting with a CKAN data portal."""

    def __init__(self, base_url: str):
        # Strip trailing slash so we can build URLs cleanly
        self.base_url = base_url.rstrip("/")

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """Make a GET request to a CKAN API endpoint and return the JSON response."""
        url = f"{self.base_url}/api/{endpoint}"
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def api_version(self) -> int:
        """Return the API version number used by this CKAN portal."""
        data = self._get("action/status_show")
        return data["result"]["ckan_version"]

    def list_datasets(self) -> list[str]:
        """Return a list of all dataset names (package IDs) on this portal."""
        data = self._get("action/package_list")
        return data["result"]

    def get_dataset_metadata(self, dataset_id: str) -> dict:
        """Return the full metadata for a dataset and its resources.

        dataset_id can be either the dataset's name (URL slug) or its UUID.
        """
        data = self._get("action/package_show", params={"id": dataset_id})
        return data["result"]

    def search_datasets(self, query: str, rows: int = 10) -> list[dict]:
        """Search for datasets matching a query string.

        Returns a list of dataset metadata dicts (up to `rows` results).
        Uses CKAN's Solr-backed search, so standard Solr query syntax works.
        """
        data = self._get("action/package_search", params={"q": query, "rows": rows})
        return data["result"]["results"]

    def list_organizations(self) -> list[str]:
        """Return a list of all organization names on this portal.

        Organizations are the publishers — each dataset belongs to exactly one.
        """
        data = self._get("action/organization_list")
        return data["result"]

    def list_groups(self) -> list[str]:
        """Return a list of all group names on this portal.

        Groups are thematic collections of datasets that cut across organizations
        (e.g. "Health", "Environment"). A dataset can belong to multiple groups.
        """
        data = self._get("action/group_list")
        return data["result"]

    def list_tags(self) -> list[str]:
        """Return a list of all tags used across datasets on this portal."""
        data = self._get("action/tag_list")
        return data["result"]

    def get_organization(self, org_id: str) -> dict:
        """Return details for an organization, including its dataset count.

        org_id can be the organization's name (URL slug) or UUID.
        """
        data = self._get(
            "action/organization_show",
            params={"id": org_id, "include_datasets": True},
        )
        return data["result"]

    def get_group(self, group_id: str) -> dict:
        """Return details for a group, including its datasets.

        group_id can be the group's name (URL slug) or UUID.
        """
        data = self._get(
            "action/group_show",
            params={"id": group_id, "include_datasets": True},
        )
        return data["result"]

    def get_tag(self, tag_id: str) -> dict:
        """Return details for a tag, including the datasets that use it.

        tag_id can be the tag's name or UUID.
        """
        data = self._get("action/tag_show", params={"id": tag_id})
        return data["result"]

    def find_resources(self, dataset_id: str, fmt: str) -> list[dict]:
        """Return resources from a dataset that match a given format (e.g. 'CSV', 'GeoJSON').

        The format comparison is case-insensitive.
        """
        meta = self.get_dataset_metadata(dataset_id)
        return [
            r for r in meta.get("resources", []) if (r.get("format") or "").upper() == fmt.upper()
        ]
