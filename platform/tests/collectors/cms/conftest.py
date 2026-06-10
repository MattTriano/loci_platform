"""
Pytest fixtures for the CMS collector tests. Fakes and test-data
helpers live in helpers.py (conftest is for fixtures only — importing
from conftest breaks once the test directory is a package).

DB-backed tests connect using these env vars (defaults in parentheses):

    CMS_TEST_PGHOST (localhost), CMS_TEST_PGPORT (5432),
    CMS_TEST_PGDATABASE (postgres), CMS_TEST_PGUSER (postgres),
    CMS_TEST_PGPASSWORD ("")

If no connection can be made, DB-backed tests are skipped. Each test
gets a throwaway schema, dropped at teardown.

NOTE: adjust the PostgresEngine import inside the `engine` fixture to
its real module path.
"""

from __future__ import annotations

import os
import uuid as uuid_module
from types import SimpleNamespace

import pytest
from loci.collectors.cms.collector import CMSCollector

from .helpers import FakeCMSClient, NoopTracker, make_spec


@pytest.fixture(scope="session")
def engine():
    try:
        from loci.db.core import PostgresEngine
    except ImportError as e:
        pytest.skip(f"Could not import PostgresEngine — fix the import in conftest.py ({e})")

    creds = SimpleNamespace(
        host=os.environ.get("CMS_TEST_PGHOST", "localhost"),
        port=int(os.environ.get("CMS_TEST_PGPORT", "54321")),
        database=os.environ.get("CMS_TEST_PGDATABASE"),
        username=os.environ.get("CMS_TEST_PGUSER"),
        password=os.environ.get("CMS_TEST_PGPASSWORD"),
    )
    eng = PostgresEngine(creds)
    try:
        eng.query("select 1 as ok")
    except Exception as e:
        pytest.skip(f"No test Postgres available: {e}")
    yield eng
    eng.close()


@pytest.fixture
def schema(engine):
    name = f"cms_test_{uuid_module.uuid4().hex[:8]}"
    engine.execute(f"create schema {name}")
    yield name
    engine.execute(f"drop schema {name} cascade")


@pytest.fixture
def warehouse(engine, schema):
    """
    Factory: given a FakeCMSSource, return (collector, spec) with the
    target table created from the collector's own generated DDL — so
    every DB test also exercises DDL ingest-readiness.
    """

    def build(source, retrieval="api", vintages=None, create_table=True):
        spec = make_spec(schema, retrieval=retrieval, vintages=vintages)
        collector = CMSCollector(engine=engine, client=FakeCMSClient(source), tracker=NoopTracker())
        if create_table:
            engine.execute(collector.generate_ddl(spec))
        return collector, spec

    return build
