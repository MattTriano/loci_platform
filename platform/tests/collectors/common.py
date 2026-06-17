"""
Shared non-fixture test code for the collector suites.

Imported by leaf suites via the package chain, e.g.:

    from ..common import NoopTracker

(This works because tests/ and tests/collectors/ are packages — pytest
derives module names by walking __init__.py upward, so leaf modules are
tests.collectors.<source>.test_* and relative parent imports resolve.)
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace


class NoopTracker:
    """Tracker stand-in: records runs, touches no database."""

    def __init__(self):
        self.runs: list[tuple[str, SimpleNamespace]] = []

    @contextlib.contextmanager
    def track(self, source, dataset_id, target_table, metadata=None):
        run = SimpleNamespace(
            metadata=metadata or {},
            rows_staged=0,
            rows_merged=0,
            rows_ingested=0,
            high_water_mark=None,
        )
        self.runs.append((dataset_id, run))
        yield run
