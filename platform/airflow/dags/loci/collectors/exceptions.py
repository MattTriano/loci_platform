from __future__ import annotations


class SchemaDriftError(Exception):
    """Source schema has diverged from the target table."""
