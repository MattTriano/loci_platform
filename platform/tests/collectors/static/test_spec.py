"""
Unit behaviors of StaticFileDatasetSpec and FileRef: validation that
catches manifest mistakes at construction time, not at collection time.
"""

from __future__ import annotations

import pytest
from loci.collectors.static.spec import FileRef, StaticFileDatasetSpec


class TestFileRefValidation:
    def test_rejects_unsupported_format(self):
        with pytest.raises(ValueError, match="file_format"):
            FileRef(url="https://x.test/f.parquet", vintage="2023", file_format="parquet")

    def test_accepts_csv_and_xlsx(self):
        FileRef(url="https://x.test/f.csv", vintage="2023", file_format="csv")
        FileRef(url="https://x.test/f.xlsx", vintage="2023", file_format="xlsx")


class TestSpecValidation:
    def test_rejects_empty_manifest(self):
        with pytest.raises(ValueError, match="empty file manifest"):
            StaticFileDatasetSpec(
                name="t", target_table="t", entity_key=["id", "vintage"], files=[]
            )

    def test_rejects_duplicate_vintages(self):
        files = [
            FileRef(url="https://x.test/a.csv", vintage="2023"),
            FileRef(url="https://x.test/b.csv", vintage="2023"),
        ]
        with pytest.raises(ValueError, match="duplicate vintages"):
            StaticFileDatasetSpec(
                name="t", target_table="t", entity_key=["id", "vintage"], files=files
            )

    def test_distinct_vintages_accepted(self):
        files = [
            FileRef(url="https://x.test/a.csv", vintage="2022"),
            FileRef(url="https://x.test/b.csv", vintage="2023"),
        ]
        spec = StaticFileDatasetSpec(
            name="t", target_table="t", entity_key=["id", "vintage"], files=files
        )
        assert [f.vintage for f in spec.files] == ["2022", "2023"]

    def test_target_schema_defaults_to_raw_data(self):
        spec = StaticFileDatasetSpec(
            name="t",
            target_table="t",
            files=[FileRef(url="https://x.test/a.csv", vintage="2023")],
        )
        assert spec.target_schema == "raw_data"
        assert spec.entity_key is None  # append-only by default
