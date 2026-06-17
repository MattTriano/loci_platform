# /loci_platform/platform/tests/collectors/threedep/conftest.py
"""
3DEP-specific fixtures. Shared fixtures (engine, schema) come from the
parent conftest; shared code (NoopTracker) from ..common.
"""

from __future__ import annotations

import pytest
from loci.collectors.threedep.collector import ThreeDEPCollector

from ..common import NoopTracker
from .helpers import SUB_TILE, FakeThreeDEPClient


@pytest.fixture
def warehouse(engine):
    """
    Factory: given a spec and its FakeThreeDEPSource, return a collector
    with the fake client, and the target table created from the
    collector's own generated DDL — so every DB test also exercises DDL
    ingest-readiness. A small sub-tile size is used so a clip to a
    sub-degree bbox visibly drops sub-tiles.
    """

    def build(spec, source, create_table=True):
        collector = ThreeDEPCollector(
            engine=engine,
            client=FakeThreeDEPClient(source),
            tracker=NoopTracker(),
            tile_size=SUB_TILE,
        )
        if create_table:
            engine.execute(collector.generate_ddl(spec))
        return collector

    return build
