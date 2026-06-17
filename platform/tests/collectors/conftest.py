# /loci_platform/platform/tests/collectors/conftest.py
"""
Shared fixtures for the collector test suites (cms, dkan, ...).

Pytest cascades these to every subdirectory automatically — leaf
conftests define only what's source-specific (typically a `warehouse`
factory) and can depend on `engine`/`schema` as if they were local.
Plain shared code (fakes, helpers) lives in common.py, not here:
conftest is for fixtures only.

DB-backed tests connect using these env vars (defaults in parentheses):

    DWH_TEST_PGHOST (localhost), DWH_TEST_PGPORT,
    DWH_TEST_PGDATABASE, DWH_TEST_PGUSER,
    DWH_TEST_PGPASSWORD

If no connection can be made, DB-backed tests are skipped. Each test
gets a throwaway schema, dropped at teardown.
"""

from __future__ import annotations

import os
import uuid as uuid_module
from types import SimpleNamespace

import pytest


@pytest.fixture(scope="session")
def engine():
    try:
        from loci.db.core import PostgresEngine
    except ImportError as e:
        pytest.skip(f"Could not import PostgresEngine — fix the import in conftest.py ({e})")

    creds = SimpleNamespace(
        host=os.environ.get("DWH_TEST_PGHOST", "localhost"),
        port=int(os.environ.get("DWH_TEST_PGPORT")),
        database=os.environ.get("DWH_TEST_PGDATABASE"),
        username=os.environ.get("DWH_TEST_PGUSER"),
        password=os.environ.get("DWH_TEST_PGPASSWORD"),
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
    name = f"collectors_test_{uuid_module.uuid4().hex[:8]}"
    engine.execute(f"create schema {name}")
    yield name
    engine.execute(f"drop schema {name} cascade")
