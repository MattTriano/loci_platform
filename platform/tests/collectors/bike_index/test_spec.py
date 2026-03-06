"""Tests for BikeIndexDatasetSpec."""

import pytest
from loci.collectors.bike_index.spec import BikeIndexDatasetSpec


class TestBikeIndexDatasetSpec:
    def test_defaults(self):
        spec = BikeIndexDatasetSpec(name="test", target_table="test_table")
        assert spec.target_schema == "raw_data"
        assert spec.entity_key == ["id"]
        assert spec.location == "Chicago, IL"
        assert spec.per_page == 100

    def test_to_search_params(self):
        spec = BikeIndexDatasetSpec(
            name="test",
            target_table="test_table",
            location="NYC",
            distance=5,
            query="trek",
        )
        params = spec.to_search_params()
        assert params == {
            "location": "NYC",
            "distance": 5,
            "stolenness": "proximity",
            "per_page": 100,
            "query": "trek",
        }

    def test_query_excluded_when_none(self):
        spec = BikeIndexDatasetSpec(
            name="test",
            target_table="test_table",
            location="Chicago, IL",
        )
        params = spec.to_search_params()
        assert "query" not in params

    def test_invalid_stolenness_raises(self):
        with pytest.raises(ValueError, match="stolenness"):
            BikeIndexDatasetSpec(
                name="test",
                target_table="test_table",
                stolenness="invalid",
            )
