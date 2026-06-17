# /loci_platform/platform/tests/collectors/cms/conftest.py
"""
CMS-specific fixtures. Shared fixtures (engine, schema) come from the
parent conftest; shared code (NoopTracker) from ..common.
"""

from __future__ import annotations

import pytest
from loci.collectors.cms.collector import CMSCollector

from ..common import NoopTracker
from .helpers import FakeCMSClient, make_spec


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
