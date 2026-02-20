from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from collectors.socrata.client import SocrataClient
from collectors.socrata.collector import SocrataCollector
from collectors.socrata.metadata import SocrataTableMetadata

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_metadata_mock(domain: str = "data.example.org"):
    """Create a mock SocrataTableMetadata."""
    meta = MagicMock(spec=SocrataTableMetadata)
    meta.table_id = "abcd-1234"
    meta.domain = domain
    meta.columns = []
    return meta


def make_batch(*rows: dict) -> list[dict]:
    """Convenience: build a batch list from row dicts."""
    return list(rows)


def columns_df(column_names: list[str]) -> pd.DataFrame:
    """Build a DataFrame mimicking information_schema.columns output."""
    return pd.DataFrame({"column_name": column_names})


def stub_table_columns(mock_engine, columns: list[str]):
    """
    Configure mock_engine.query to return the given columns when the
    preflight check queries information_schema.columns, while still
    returning an empty DataFrame for other queries.
    """
    info_schema_result = columns_df(columns)

    def query_side_effect(sql, *args, **kwargs):
        if "information_schema.columns" in sql:
            return info_schema_result
        return pd.DataFrame()

    mock_engine.query.side_effect = query_side_effect


def attach_mock_client(collector, pages: list[list[dict]]):
    """Inject a mock SocrataClient that returns the given pages."""
    mock_client = MagicMock(spec=SocrataClient)
    mock_client.paginate.return_value = iter(pages)
    collector._client = mock_client
    return mock_client


class FakeStager:
    """
    A lightweight fake for StagedIngest that tracks write_batch calls
    and lets tests set rows_staged / rows_merged.
    """

    def __init__(self):
        self.batches: list[list[dict]] = []
        self.rows_staged = 0
        self.rows_merged = 0

    def write_batch(self, rows: list[dict]) -> int:
        self.batches.append(rows)
        self.rows_staged += len(rows)
        return len(rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if self.rows_merged == 0:
            self.rows_merged = self.rows_staged
        return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_engine():
    """A mock PostgresEngine with staged_ingest returning a FakeStager."""
    engine = MagicMock()
    engine.execute.return_value = None
    engine.query.return_value = pd.DataFrame({"column_name": []})

    engine._stagers = []

    def make_stager(**kwargs):
        stager = FakeStager()
        engine._stagers.append(stager)
        return stager

    engine.staged_ingest.side_effect = make_stager
    return engine


@pytest.fixture
def collector(mock_engine):
    """SocrataCollector with mocked engine."""
    return SocrataCollector(engine=mock_engine, page_size=100)
