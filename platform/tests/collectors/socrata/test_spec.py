from __future__ import annotations

import pytest
from loci.collectors.socrata.spec import SocrataDatasetSpec


class TestSpecMaxRowsValidation:
    def _base_kwargs(self):
        return {
            "name": "test",
            "dataset_id": "abcd-1234",
            "target_table": "test",
        }

    def test_accepts_positive_max_rows(self):
        spec = SocrataDatasetSpec(**self._base_kwargs(), max_rows=1000)
        assert spec.max_rows == 1000

    def test_accepts_none_max_rows(self):
        spec = SocrataDatasetSpec(**self._base_kwargs(), max_rows=None)
        assert spec.max_rows is None

    def test_rejects_zero_max_rows(self):
        with pytest.raises(ValueError, match="max_rows"):
            SocrataDatasetSpec(**self._base_kwargs(), max_rows=0)

    def test_rejects_negative_max_rows(self):
        with pytest.raises(ValueError, match="max_rows"):
            SocrataDatasetSpec(**self._base_kwargs(), max_rows=-5)
