"""
Unit behaviors of StaticFileClient that need neither HTTP nor a
database: CSV/XLSX parsing, column sanitization, encoding handling
(the things that silently corrupt data when they go wrong), the
HTML-response guard, and the file:// escape hatch.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from loci.collectors.static.client import (
    StaticFileClient,
    StaticFileDownloadError,
    sanitize_column_name,
)
from loci.collectors.static.spec import FileRef

from .helpers import FakeStaticFileClient, csv_bytes


class TestSanitizeColumnName:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("Hospital Name ", "hospital_name"),
            ("CCN", "ccn"),
            ("Net Revenue ($)", "net_revenue"),
            ("sys_id", "sys_id"),
            ("  ", "unnamed"),
        ],
    )
    def test_normalization(self, raw, expected):
        assert sanitize_column_name(raw) == expected


class TestCsvParsing:
    def _rows(self, data: bytes, **ref_overrides):
        ref = FileRef(url="https://x.test/f.csv", vintage="2023", **ref_overrides)
        client = FakeStaticFileClient({ref.url: data})
        return list(client.iter_rows(ref))

    def test_values_are_stripped_strings_with_sanitized_keys(self):
        data = csv_bytes(["Sys ID", "Sys Name"], [["0895", "  Adena "]])
        rows = self._rows(data)
        assert rows == [{"sys_id": "0895", "sys_name": "Adena"}]

    def test_leading_zeros_survive(self):
        data = csv_bytes(["ccn"], [["010001"], ["0895"]])
        assert [r["ccn"] for r in self._rows(data)] == ["010001", "0895"]

    def test_utf8_bom_does_not_corrupt_first_column_name(self):
        data = "\ufeffccn,name\n0895,Adena\n".encode()
        rows = self._rows(data)
        assert "ccn" in rows[0]

    def test_cp1252_en_dash_decodes_with_declared_encoding(self):
        data = csv_bytes(["name"], [["Example Health \u2013 Metro"]], encoding="cp1252")
        rows = self._rows(data, encoding="cp1252")
        assert rows[0]["name"] == "Example Health \u2013 Metro"

    def test_wrong_encoding_fails_loudly_not_silently(self):
        # The AHRQ lesson: a cp1252 byte under the utf-8 default must
        # raise, never silently mangle.
        data = csv_bytes(["name"], [["Example Health \u2013 Metro"]], encoding="cp1252")
        with pytest.raises(UnicodeDecodeError):
            self._rows(data)  # default encoding utf-8-sig

    def test_skip_rows_discards_preamble(self):
        data = b"Some Title\nGenerated 2026-01-01\nccn,name\n0895,Adena\n"
        rows = self._rows(data, skip_rows=2)
        assert rows == [{"ccn": "0895", "name": "Adena"}]


class TestXlsxParsing:
    def test_cell_rendering(self):
        openpyxl = pytest.importorskip("openpyxl")
        import datetime

        from .helpers import xlsx_bytes

        data = xlsx_bytes(
            ["ID", "Beds", "Rate", "Updated", "Note"],
            [["A1", 470.0, 35.45, datetime.date(2023, 1, 2), None]],
        )
        ref = FileRef(url="https://x.test/f.xlsx", vintage="2023", file_format="xlsx")
        client = FakeStaticFileClient({ref.url: data})
        (row,) = list(client.iter_rows(ref))
        assert row == {
            "id": "A1",
            "beds": "470",  # integral float loses Excel's trailing .0
            "rate": "35.45",
            # Excel has no date-only type; openpyxl returns a midnight
            # datetime and the client renders it faithfully as ISO 8601.
            "updated": "2023-01-02T00:00:00",
            "note": "",  # None becomes empty string
        }


class TestHtmlGuard:
    def _response(self, content_type: str, body: bytes) -> SimpleNamespace:
        return SimpleNamespace(headers={"Content-Type": content_type}, content=body)

    def test_html_content_type_raises(self):
        with pytest.raises(StaticFileDownloadError, match="HTML"):
            StaticFileClient._raise_if_html(
                "https://x.test/f.csv", self._response("text/html", b"<html></html>")
            )

    def test_html_body_raises_even_with_lying_content_type(self):
        with pytest.raises(StaticFileDownloadError, match="HTML"):
            StaticFileClient._raise_if_html(
                "https://x.test/f.csv", self._response("text/csv", b"  <!DOCTYPE html><html>")
            )

    def test_csv_passes(self):
        StaticFileClient._raise_if_html(
            "https://x.test/f.csv", self._response("text/csv", b"a,b\n1,2\n")
        )


class TestFileUrlEscapeHatch:
    def test_file_url_roundtrip(self, tmp_path):
        path = tmp_path / "manual-download.csv"
        path.write_bytes(csv_bytes(["ccn", "name"], [["0895", "Adena"]]))
        ref = FileRef(url=f"file://{path}", vintage="2023")
        client = StaticFileClient(delay_seconds=0)  # real client; no HTTP happens
        rows = list(client.iter_rows(ref))
        assert rows == [{"ccn": "0895", "name": "Adena"}]
