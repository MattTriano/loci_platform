from __future__ import annotations

import csv
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def parse_csv(
    filepath: str | Path,
    batch_size: int = 10000,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> Iterator[list[dict[str, Any]]]:
    """
    Stream-parse a CSV file into batches of row dicts.

    Args:
        filepath:   Path to .csv file.
        batch_size: Rows per yielded batch.
        delimiter:  Field delimiter.
        encoding:   File encoding.

    Yields:
        Lists of dicts, one dict per row, keyed by header names.

    Usage:
        for batch in parse_csv("data.csv"):
            stager.write_batch(batch)
    """
    filepath = Path(filepath)
    batch: list[dict[str, Any]] = []

    with open(filepath, encoding=encoding, newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            batch.append({k: (v if v != "" else None) for k, v in row.items()})

            if len(batch) >= batch_size:
                yield batch
                batch = []

    if batch:
        yield batch
