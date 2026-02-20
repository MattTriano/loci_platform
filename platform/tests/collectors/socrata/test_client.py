from __future__ import annotations

from unittest.mock import MagicMock, patch

from collectors.socrata.client import SocrataClient


class TestSocrataClient:
    @patch("collectors.socrata.client.requests.Session")
    def test_paginate_single_page(self, MockSession):
        mock_session = MagicMock()
        MockSession.return_value = mock_session

        page_data = [{"id": "1"}, {"id": "2"}]
        mock_resp = MagicMock()
        mock_resp.json.return_value = page_data
        mock_session.get.return_value = mock_resp

        client = SocrataClient(page_size=100)
        client._session = mock_session

        pages = list(client.paginate("example.com", "abcd-1234"))
        assert len(pages) == 1
        assert pages[0] == page_data

    @patch("collectors.socrata.client.requests.Session")
    def test_paginate_stops_after_partial_page(self, MockSession):
        mock_session = MagicMock()
        MockSession.return_value = mock_session

        page1 = [{"id": "1"}, {"id": "2"}]
        page2 = [{"id": "3"}]  # partial → last page
        mock_resp1 = MagicMock()
        mock_resp1.json.return_value = page1
        mock_resp2 = MagicMock()
        mock_resp2.json.return_value = page2
        mock_session.get.side_effect = [mock_resp1, mock_resp2]

        client = SocrataClient(page_size=2)
        client._session = mock_session

        pages = list(client.paginate("example.com", "abcd-1234"))
        assert len(pages) == 2
        assert len(pages[0]) == 2
        assert len(pages[1]) == 1

    def test_query_builds_soda_params(self):
        client = SocrataClient()
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"a": 1}]
        mock_resp.headers = {}
        mock_session.get.return_value = mock_resp
        client._session = mock_session

        result = client.query(
            "example.com",
            "abcd-1234",
            select="col1, col2",
            where="col1 > 5",
            order="col1 DESC",
            limit=10,
        )

        params = mock_session.get.call_args[1]["params"]
        assert params["$select"] == "col1, col2"
        assert params["$where"] == "col1 > 5"
        assert params["$order"] == "col1 DESC"
        assert params["$limit"] == "10"
        assert result == [{"a": 1}]
