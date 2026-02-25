"""Tests for parse_csv."""

import csv
from pathlib import Path

from loci.parsers.csv_parser import parse_csv


def _write_csv(tmp_path: Path, headers: list[str], rows: list[list], **kwargs) -> Path:
    """Write a CSV file and return its path."""
    filepath = tmp_path / "test.csv"
    with open(filepath, "w", newline="", encoding=kwargs.get("encoding", "utf-8")) as f:
        writer = csv.writer(f, delimiter=kwargs.get("delimiter", ","))
        writer.writerow(headers)
        writer.writerows(rows)
    return filepath


def _collect_all_rows(batches) -> list[dict]:
    """Drain the batch iterator into a flat list of rows."""
    return [row for batch in batches for row in batch]


# ── Basic parsing ──────────────────────────────────────────────────


class TestBasicParsing:
    def test_single_row(self, tmp_path):
        filepath = _write_csv(tmp_path, ["name", "age"], [["Alice", "30"]])
        rows = _collect_all_rows(parse_csv(filepath))

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == "30"

    def test_multiple_rows(self, tmp_path):
        filepath = _write_csv(
            tmp_path,
            ["id", "city"],
            [["1", "Chicago"], ["2", "Denver"], ["3", "Portland"]],
        )
        rows = _collect_all_rows(parse_csv(filepath))

        assert len(rows) == 3
        assert rows[0]["city"] == "Chicago"
        assert rows[2]["city"] == "Portland"

    def test_empty_field_values(self, tmp_path):
        filepath = tmp_path / "blanks.csv"
        filepath.write_text("a,b,c\n1,,3\n")
        rows = _collect_all_rows(parse_csv(filepath))
        assert rows[0]["b"] is None

    def test_rows_are_keyed_by_header(self, tmp_path):
        filepath = _write_csv(
            tmp_path,
            ["first_name", "last_name"],
            [["Jane", "Doe"]],
        )
        rows = _collect_all_rows(parse_csv(filepath))

        assert set(rows[0].keys()) == {"first_name", "last_name"}


# ── Null handling ──────────────────────────────────────────────────


class TestNullHandling:
    def test_empty_strings_become_none(self, tmp_path):
        filepath = tmp_path / "nulls.csv"
        filepath.write_text("name,age,score\nalice,30,95.5\nbob,,\ncharlie,25,\n")

        rows = _collect_all_rows(parse_csv(filepath))

        assert rows[0] == {"name": "alice", "age": "30", "score": "95.5"}
        assert rows[1] == {"name": "bob", "age": None, "score": None}
        assert rows[2] == {"name": "charlie", "age": "25", "score": None}

    def test_whitespace_values_not_treated_as_null(self, tmp_path):
        filepath = tmp_path / "spaces.csv"
        filepath.write_text("name,val\nalice, \n")

        rows = _collect_all_rows(parse_csv(filepath))

        assert rows[0]["val"] == " "

    def test_all_fields_empty(self, tmp_path):
        filepath = tmp_path / "all_empty.csv"
        filepath.write_text("a,b,c\n,,\n")

        rows = _collect_all_rows(parse_csv(filepath))

        assert rows[0] == {"a": None, "b": None, "c": None}


# ── Batching ───────────────────────────────────────────────────────


class TestBatching:
    def test_rows_split_into_batches(self, tmp_path):
        filepath = _write_csv(
            tmp_path,
            ["id"],
            [[str(i)] for i in range(7)],
        )
        batches = list(parse_csv(filepath, batch_size=3))

        assert len(batches) == 3  # 3 + 3 + 1
        assert len(batches[0]) == 3
        assert len(batches[1]) == 3
        assert len(batches[2]) == 1

    def test_exact_multiple_of_batch_size(self, tmp_path):
        filepath = _write_csv(
            tmp_path,
            ["id"],
            [[str(i)] for i in range(6)],
        )
        batches = list(parse_csv(filepath, batch_size=3))

        assert len(batches) == 2
        assert all(len(b) == 3 for b in batches)

    def test_batch_size_larger_than_row_count(self, tmp_path):
        filepath = _write_csv(
            tmp_path,
            ["id"],
            [["1"], ["2"]],
        )
        batches = list(parse_csv(filepath, batch_size=100))

        assert len(batches) == 1
        assert len(batches[0]) == 2


# ── Delimiters and encoding ────────────────────────────────────────


class TestDelimitersAndEncoding:
    def test_tab_delimiter(self, tmp_path):
        filepath = tmp_path / "tabs.tsv"
        filepath.write_text("name\tage\nAlice\t30\n")

        rows = _collect_all_rows(parse_csv(filepath, delimiter="\t"))

        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == "30"

    def test_pipe_delimiter(self, tmp_path):
        filepath = tmp_path / "pipes.csv"
        filepath.write_text("a|b\n1|2\n")

        rows = _collect_all_rows(parse_csv(filepath, delimiter="|"))

        assert rows[0] == {"a": "1", "b": "2"}

    def test_latin1_encoding(self, tmp_path):
        filepath = tmp_path / "latin.csv"
        filepath.write_bytes("name,city\nJosé,São Paulo\n".encode("latin-1"))

        rows = _collect_all_rows(parse_csv(filepath, encoding="latin-1"))

        assert rows[0]["name"] == "José"
        assert rows[0]["city"] == "São Paulo"


# ── Edge cases ─────────────────────────────────────────────────────


class TestEdgeCases:
    def test_fields_with_commas_are_preserved(self, tmp_path):
        filepath = _write_csv(
            tmp_path,
            ["name", "address"],
            [["Alice", "123 Main St, Apt 4"]],
        )
        rows = _collect_all_rows(parse_csv(filepath))

        assert rows[0]["address"] == "123 Main St, Apt 4"

    def test_fields_with_newlines_are_preserved(self, tmp_path):
        filepath = _write_csv(
            tmp_path,
            ["name", "notes"],
            [["Alice", "line one\nline two"]],
        )
        rows = _collect_all_rows(parse_csv(filepath))

        assert rows[0]["notes"] == "line one\nline two"

    def test_empty_field_values(self, tmp_path):
        filepath = tmp_path / "blanks.csv"
        filepath.write_text("a,b,c\n1,,3\n")

        rows = _collect_all_rows(parse_csv(filepath))

        assert rows[0]["b"] is None

    def test_whitespace_in_values_is_preserved(self, tmp_path):
        filepath = tmp_path / "spaces.csv"
        filepath.write_text("name,value\n  padded  ,  also  \n")

        rows = _collect_all_rows(parse_csv(filepath))

        assert rows[0]["name"] == "  padded  "
        assert rows[0]["value"] == "  also  "
