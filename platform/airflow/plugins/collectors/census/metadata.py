"""
CensusMetadata — a lightweight tool for exploring Census Bureau API datasets,
variable groups, variables, and geographies from a Jupyter notebook.

Usage:
    from census_metadata import CensusMetadata
    cm = CensusMetadata(api_key="YOUR_KEY")

    # What datasets exist?
    cm.list_datasets()                        # all datasets
    cm.list_datasets("acs")                   # filter by keyword

    # What years are available for a dataset?
    cm.list_vintages("acs/acs5")

    # What groups (tables) exist in a dataset+year?
    cm.list_groups("acs/acs5", 2022)          # all groups
    cm.list_groups("acs/acs5", 2022, "occupation")  # search by keyword

    # What variables are in a group?
    cm.list_variables("acs/acs5", 2022, group="B24010")

    # What geographies are available?
    cm.list_geographies("acs/acs5", 2022)
"""

from functools import lru_cache

import pandas as pd
import requests


class CensusMetadata:
    BASE = "https://api.census.gov/data"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self._session = requests.Session()

    # ------------------------------------------------------------------ #
    #  Internal helpers
    # ------------------------------------------------------------------ #

    def _get_json(self, url: str) -> dict:
        """Fetch JSON from a URL, raising on errors."""
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json()

    @lru_cache(maxsize=1)
    def _dataset_catalog(self) -> list[dict]:
        """Fetch and cache the full dataset catalog (api.census.gov/data.json)."""
        data = self._get_json(f"{self.BASE}.json")
        return data.get("dataset", [])

    # ------------------------------------------------------------------ #
    #  Public methods
    # ------------------------------------------------------------------ #

    def list_datasets(self, keyword: str | None = None, max_len: int = 200) -> pd.DataFrame:
        """
        List available Census API datasets.

        Parameters
        ----------
        keyword : str, optional
            Case-insensitive substring filter applied to dataset title and description.

        Returns a DataFrame with columns: title, name, vintage, description.
        """
        rows = []
        for ds in self._dataset_catalog():
            name = "/".join(ds.get("c_dataset", []))
            vintage = ds.get("c_vintage")
            title = ds.get("title", "")
            desc = ds.get("description", "")
            is_available = ds.get("c_isAvailable", False)

            if not is_available:
                continue

            if keyword and keyword.lower() not in f"{title} {desc} {name}".lower():
                continue

            rows.append(
                {
                    "title": title,
                    "name": name,
                    "vintage": vintage,
                    "description": desc[:max_len] + "..." if len(desc) > max_len else desc,
                }
            )

        df = pd.DataFrame(rows).convert_dtypes()
        if not df.empty:
            df = df.sort_values(["name", "vintage"]).reset_index(drop=True)
        return df

    def list_vintages(self, dataset_name: str) -> list:
        """
        List available years (vintages) for a given dataset name.

        Parameters
        ----------
        dataset_name : str
            e.g. "acs/acs5", "dec/pl", "cbp"
        """
        vintages = []
        for ds in self._dataset_catalog():
            name = "/".join(ds.get("c_dataset", []))
            if name == dataset_name and ds.get("c_isAvailable", False):
                v = ds.get("c_vintage")
                if v is not None:
                    vintages.append(v)
        return sorted(set(vintages))

    def list_groups(
        self, dataset_name: str, vintage: int, keyword: str | None = None
    ) -> pd.DataFrame:
        """
        List variable groups (tables) for a dataset + vintage.

        Parameters
        ----------
        dataset_name : str
            e.g. "acs/acs5"
        vintage : int
            e.g. 2022
        keyword : str, optional
            Case-insensitive substring filter on group name/description.

        Returns a DataFrame with columns: group, description.
        """
        url = f"{self.BASE}/{vintage}/{dataset_name}/groups.json"
        data = self._get_json(url)
        rows = []
        for g in data.get("groups", []):
            name = g.get("name", "")
            desc = g.get("description", "")
            if keyword and keyword.lower() not in f"{name} {desc}".lower():
                continue
            rows.append({"group": name, "description": desc})

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("group").reset_index(drop=True)
        return df

    def list_variables(
        self,
        dataset_name: str,
        vintage: int,
        group: str | None = None,
        keyword: str | None = None,
    ) -> pd.DataFrame:
        """
        List variables in a dataset, optionally filtered by group or keyword.

        Parameters
        ----------
        dataset_name : str
        vintage : int
        group : str, optional
            A group code like "B24010". If given, fetches only that group's variables.
        keyword : str, optional
            Case-insensitive substring filter on variable name or label.

        Returns a DataFrame with columns: variable, label, group, concept.
        """
        if group:
            url = f"{self.BASE}/{vintage}/{dataset_name}/groups/{group}.json"
            data = self._get_json(url)
            variables = data.get("variables", {})
        else:
            url = f"{self.BASE}/{vintage}/{dataset_name}/variables.json"
            data = self._get_json(url)
            variables = data.get("variables", {})

        rows = []
        for var_name, info in variables.items():
            label = info.get("label", "")
            grp = info.get("group", "")
            concept = info.get("concept", "")
            pred_type = info.get("predicateType", "")

            if keyword and keyword.lower() not in f"{var_name} {label} {concept}".lower():
                continue

            rows.append(
                {
                    "variable": var_name,
                    "label": label,
                    "group": grp,
                    "concept": concept,
                    "predicate_type": pred_type,
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("variable").reset_index(drop=True)
        return df

    def list_geographies(self, dataset_name: str, vintage: int) -> pd.DataFrame:
        """
        List available geographies for a dataset + vintage.

        Returns a DataFrame with columns: name, hierarchy, wildcard, requires.
        """
        url = f"{self.BASE}/{vintage}/{dataset_name}/geography.json"
        data = self._get_json(url)
        rows = []
        for geo in data.get("fips", []):
            name = geo.get("name", "")
            hierarchy = geo.get("geoLevelDisplay", "")
            wildcard = ", ".join(geo.get("wildcard", []))
            requires = ", ".join(geo.get("requires", []))
            rows.append(
                {
                    "name": name,
                    "hierarchy": hierarchy,
                    "wildcard": wildcard,
                    "requires": requires,
                }
            )
        return pd.DataFrame(rows)
