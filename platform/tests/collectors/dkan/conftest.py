# /loci_platform/platform/tests/collectors/dkan/conftest.py
"""
DKAN-specific fixtures. Shared fixtures (engine, schema) come from the
parent conftest; shared code (NoopTracker) from ..common.
"""

from __future__ import annotations

import pytest
from loci.collectors.dkan.collector import DKANCollector

from ..common import NoopTracker
from .helpers import FakeDKANClient


@pytest.fixture
def warehouse(engine):
    """
    Factory: given a spec and its FakeDKANSource, return a collector
    with the fake client pre-seeded for the spec's portal, and the
    target table created from the collector's own generated DDL — so
    every DB test also exercises DDL ingest-readiness.
    """

    def build(spec, source, create_table=True):
        collector = DKANCollector(
            engine=engine,
            tracker=NoopTracker(),
            clients={spec.base_url: FakeDKANClient(spec.base_url, source)},
        )
        if create_table:
            engine.execute(collector.generate_ddl(spec))
        return collector

    return build
