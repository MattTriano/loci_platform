"""
Static-collector-specific fixtures. Shared fixtures (engine, schema)
come from the parent conftest; shared code (NoopTracker) from ..common.
"""

from __future__ import annotations

import pytest
from loci.collectors.static.collector import StaticFileCollector

from ..common import NoopTracker
from .helpers import FakeStaticFileClient, default_files, make_spec


@pytest.fixture
def warehouse(engine, schema):
    """
    Factory: given a {url: bytes} source, return (collector, spec) with
    the target table created from the collector's own generated DDL —
    so every DB test also exercises DDL ingest-readiness.
    """

    def build(files=None, spec=None, create_table=True, tracker=None, **spec_overrides):
        spec = spec or make_spec(schema, **spec_overrides)
        collector = StaticFileCollector(
            engine=engine,
            client=FakeStaticFileClient(files if files is not None else default_files()),
            tracker=tracker if tracker is not None else NoopTracker(),
        )
        if create_table:
            engine.execute(collector.generate_ddl(spec))
        return collector, spec

    return build
