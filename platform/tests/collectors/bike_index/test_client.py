"""Tests for BikeIndexClient and BikeIndexSearchParams."""

from unittest.mock import MagicMock, patch

import pytest
import requests
from loci.collectors.bike_index.client import (
    BikeIndexClient,
    BikeIndexSearchParams,
    RateLimitedError,
    ServerError,
)
from tenacity import RetryError


class TestBikeIndexSearchParams:
    def test_to_params_defaults(self):
        p = BikeIndexSearchParams()
        assert p.to_params() == {
            "location": "Chicago, IL",
            "distance": 10,
            "stolenness": "proximity",
            "per_page": 100,
        }

    def test_to_params_with_query(self):
        p = BikeIndexSearchParams(query="red trek")
        params = p.to_params()
        assert params["query"] == "red trek"

    def test_query_excluded_when_none(self):
        p = BikeIndexSearchParams(query=None)
        assert "query" not in p.to_params()


class TestBikeIndexClient:
    def test_auth_header_set(self):
        client = BikeIndexClient(access_token="test-token")
        assert client._session.headers["Authorization"] == "Bearer test-token"
        client.close()

    def test_no_auth_header_without_token(self):
        client = BikeIndexClient()
        assert "Authorization" not in client._session.headers
        client.close()

    def test_context_manager(self):
        with BikeIndexClient() as client:
            assert client._session is not None

    @patch.object(requests.Session, "request")
    def test_request_success(self, mock_request):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"bikes": []}
        mock_request.return_value = resp

        with BikeIndexClient() as client:
            result = client._request("GET", "/search")

        assert result == {"bikes": []}

    @patch.object(requests.Session, "request")
    def test_request_raises_rate_limited_on_429(self, mock_request):
        resp = MagicMock()
        resp.status_code = 429
        resp.headers = {"Retry-After": "5"}
        mock_request.return_value = resp

        with BikeIndexClient() as client:
            with pytest.raises(RetryError) as exc_info:
                client._request("GET", "/search")

            last_exc = exc_info.value.last_attempt.exception()
            assert isinstance(last_exc, RateLimitedError)
            assert last_exc.retry_after == 5.0
            assert mock_request.call_count == 4

    @patch.object(requests.Session, "request")
    def test_request_raises_server_error_on_500(self, mock_request):
        resp = MagicMock()
        resp.status_code = 503
        mock_request.return_value = resp

        with BikeIndexClient() as client:
            with pytest.raises(RetryError) as exc_info:
                client._request("GET", "/search")

            last_exc = exc_info.value.last_attempt.exception()
            assert isinstance(last_exc, ServerError)
            assert last_exc.status_code == 503
            assert mock_request.call_count == 4

    @patch.object(requests.Session, "request")
    def test_get_bike_unwraps_bike_key(self, mock_request):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"bike": {"id": 123, "title": "Test"}}
        mock_request.return_value = resp

        with BikeIndexClient() as client:
            result = client.get_bike(123)

        assert result == {"id": 123, "title": "Test"}

    @patch.object(requests.Session, "request")
    def test_search_all_paginates(self, mock_request):
        page1_resp = MagicMock()
        page1_resp.status_code = 200
        page1_resp.json.return_value = {"bikes": [{"id": i} for i in range(100)]}

        page2_resp = MagicMock()
        page2_resp.status_code = 200
        page2_resp.json.return_value = {"bikes": [{"id": 100}]}

        mock_request.side_effect = [page1_resp, page2_resp]

        search = BikeIndexSearchParams(per_page=100)
        with BikeIndexClient() as client:
            pages = list(client.search_all(search))

        assert len(pages) == 2
        assert len(pages[0]) == 100
        assert len(pages[1]) == 1

    @patch.object(requests.Session, "request")
    def test_search_all_stops_on_empty(self, mock_request):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"bikes": []}
        mock_request.return_value = resp

        search = BikeIndexSearchParams()
        with BikeIndexClient() as client:
            pages = list(client.search_all(search))

        assert pages == []
