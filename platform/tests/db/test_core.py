import json
from unittest.mock import MagicMock, patch
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
import pytest

from db.core import PostgresEngine, DatabaseCredentials


@pytest.fixture
def sample_creds():
    return DatabaseCredentials(
        host="db.example.com",
        port=5432,
        database="chicago",
        username="etl_user",
        password="s3cret!@#",
    )


@pytest.fixture
def mock_cursor():
    cur = MagicMock()
    cur.description = [("col_a",), ("col_b",)]
    cur.fetchall.return_value = [("val1", "val2"), ("val3", "val4")]
    cur.fetchone.return_value = None  # no geometry by default
    cur.rowcount = 2
    cur.close.return_value = None
    return cur


@pytest.fixture
def mock_conn(mock_cursor):
    conn = MagicMock()
    conn.closed = False
    conn.cursor.return_value = mock_cursor
    conn.commit.return_value = None
    conn.rollback.return_value = None
    return conn


@pytest.fixture
def engine(mock_conn, sample_creds):
    eng = PostgresEngine(sample_creds)
    eng._conn = mock_conn
    return eng


def _get_execute_sql_strings(mock_cursor) -> list[str]:
    """Extract all SQL strings passed to cursor.execute()."""
    return [
        str(call_args[0][0])
        for call_args in mock_cursor.execute.call_args_list
        if call_args[0]
    ]


def _find_sql_containing(mock_cursor, fragment: str) -> list[str]:
    """Return all executed SQL strings that contain the given fragment."""
    return [s for s in _get_execute_sql_strings(mock_cursor) if fragment in s]


class TestDatabaseCredentials:
    def test_direct_construction(self, sample_creds):
        assert sample_creds.host == "db.example.com"
        assert sample_creds.port == 5432
        assert sample_creds.database == "chicago"
        assert sample_creds.username == "etl_user"
        assert sample_creds.password == "s3cret!@#"
        assert sample_creds.driver == "postgresql"

    def test_default_driver(self):
        creds = DatabaseCredentials(
            host="localhost",
            port=5432,
            database="test",
            username="u",
            password="p",
        )
        assert creds.driver == "postgresql"

    def test_custom_driver(self):
        creds = DatabaseCredentials(
            host="localhost",
            port=5432,
            database="test",
            username="u",
            password="p",
            driver="postgresql+psycopg2",
        )
        assert creds.driver == "postgresql+psycopg2"

    # -- connection_string -------------------------------------------------

    def test_connection_string(self, sample_creds):
        cs = sample_creds.connection_string
        assert cs.startswith("postgresql://etl_user:")
        assert "@db.example.com:5432/chicago" in cs
        assert quote_plus("s3cret!@#") in cs

    def test_connection_string_simple_password(self):
        creds = DatabaseCredentials(
            host="localhost",
            port=5432,
            database="mydb",
            username="admin",
            password="plainpass",
        )
        assert (
            creds.connection_string
            == "postgresql://admin:plainpass@localhost:5432/mydb"
        )

    # -- redacted_connection_string ----------------------------------------

    def test_redacted_connection_string(self, sample_creds):
        rcs = sample_creds.redacted_connection_string
        assert "s3cret" not in rcs
        assert "db.example.com" not in rcs
        assert rcs == "postgresql://etl_user:****@****:5432/chicago"

    # -- __str__ / __repr__ -----------------------------------------------

    def test_str_redacts_sensitive_fields(self, sample_creds):
        s = str(sample_creds)
        assert "s3cret" not in s
        assert "db.example.com" not in s
        assert "etl_user" in s
        assert "chicago" in s

    def test_repr_matches_str(self, sample_creds):
        assert repr(sample_creds) == str(sample_creds)


class TestPgRetry:
    def test_retries_on_operational_error(self, engine, mock_conn, mock_cursor):
        # First call: main query fails. Retry succeeds.
        # Each call may also trigger a geometry-detection execute, so we
        # count only calls whose SQL matches our query.
        mock_cursor.execute.side_effect = [
            psycopg2.OperationalError("connection reset"),
            None,  # retry: main query succeeds
            None,  # geometry detection query
        ]
        engine.query("SELECT 1")
        main_query_calls = [
            c for c in mock_cursor.execute.call_args_list if c[0][0] == "SELECT 1"
        ]
        assert len(main_query_calls) == 2

    def test_retries_on_interface_error(self, engine, mock_conn, mock_cursor):
        mock_cursor.execute.side_effect = [
            psycopg2.InterfaceError("connection closed"),
            None,
            None,
        ]
        engine.query("SELECT 1")
        main_query_calls = [
            c for c in mock_cursor.execute.call_args_list if c[0][0] == "SELECT 1"
        ]
        assert len(main_query_calls) == 2

    def test_no_retry_on_programming_error(self, engine, mock_cursor):
        mock_cursor.execute.side_effect = psycopg2.ProgrammingError("syntax error")
        with pytest.raises(psycopg2.ProgrammingError):
            engine.query("SELECT bad syntax")
        assert mock_cursor.execute.call_count == 1

    def test_exhausts_retries(self, engine, mock_conn, mock_cursor):
        mock_cursor.execute.side_effect = psycopg2.OperationalError("down")
        with pytest.raises(psycopg2.OperationalError):
            engine.query("SELECT 1")
        assert mock_cursor.execute.call_count == 3

    def test_retry_applies_to_execute(self, engine, mock_conn, mock_cursor):
        mock_cursor.execute.side_effect = [
            psycopg2.OperationalError("timeout"),
            None,
        ]
        engine.execute("DROP TABLE foo")
        assert mock_cursor.execute.call_count == 2


class TestPostgresEngineParameterizedQueries:
    def test_query_with_dict_params(self, engine, mock_cursor):
        params = {"source": "socrata", "dataset": "abc-1234"}
        sql = "SELECT * FROM log WHERE source = %(source)s AND dataset = %(dataset)s"
        engine.query(sql, params=params)
        # Find the call for our specific SQL (ignoring geometry detection)
        matching = [c for c in mock_cursor.execute.call_args_list if c[0][0] == sql]
        assert len(matching) == 1
        assert matching[0][0][1] == params

    def test_query_with_tuple_params(self, engine, mock_cursor):
        sql = "SELECT * FROM log WHERE source = %s"
        engine.query(sql, params=("socrata",))
        matching = [c for c in mock_cursor.execute.call_args_list if c[0][0] == sql]
        assert len(matching) == 1
        assert matching[0][0][1] == ("socrata",)

    def test_query_without_params(self, engine, mock_cursor):
        engine.query("SELECT 1")
        matching = [
            c for c in mock_cursor.execute.call_args_list if c[0][0] == "SELECT 1"
        ]
        assert len(matching) == 1

    def test_query_returns_dataframe(self, engine, mock_cursor):
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "alice"), (2, "bob")]
        df = engine.query(
            "SELECT id, name FROM users WHERE active = %s", params=(True,)
        )
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 2

    def test_execute_with_params(self, engine, mock_cursor):
        engine.execute(
            "UPDATE foo SET bar = %(val)s WHERE id = %(id)s",
            params={"val": 42, "id": 1},
        )
        mock_cursor.execute.assert_called_once_with(
            "UPDATE foo SET bar = %(val)s WHERE id = %(id)s",
            {"val": 42, "id": 1},
        )

    def test_execute_without_params(self, engine, mock_cursor):
        engine.execute("TRUNCATE TABLE foo")
        mock_cursor.execute.assert_called_once_with("TRUNCATE TABLE foo", None)


class TestPostgresEngineQueryBatchesParams:
    def test_query_batches_passes_params(self, engine, mock_conn):
        batch_cursor = MagicMock()
        batch_cursor.description = [("x",)]
        batch_cursor.fetchmany.side_effect = [[(1,), (2,)], []]
        mock_conn.cursor.return_value = batch_cursor

        params = {"status": "active"}
        batches = list(
            engine.query_batches(
                "SELECT x FROM t WHERE status = %(status)s",
                params=params,
                batch_size=100,
            )
        )
        batch_cursor.execute.assert_called_once_with(
            "SELECT x FROM t WHERE status = %(status)s",
            {"status": "active"},
        )
        assert len(batches) == 1
        assert batches[0] == [{"x": 1}, {"x": 2}]

    def test_query_batches_as_dataframe(self, engine, mock_conn):
        batch_cursor = MagicMock()
        batch_cursor.description = [("a",), ("b",)]
        batch_cursor.fetchmany.side_effect = [[(1, 2)], []]
        mock_conn.cursor.return_value = batch_cursor

        batches = list(engine.query_batches("SELECT a, b FROM t", as_dicts=False))
        assert len(batches) == 1
        assert isinstance(batches[0], pd.DataFrame)
        assert list(batches[0].columns) == ["a", "b"]

    def test_query_batches_no_params(self, engine, mock_conn):
        batch_cursor = MagicMock()
        batch_cursor.description = [("x",)]
        batch_cursor.fetchmany.side_effect = [[]]
        mock_conn.cursor.return_value = batch_cursor

        list(engine.query_batches("SELECT 1"))
        batch_cursor.execute.assert_called_once_with("SELECT 1", None)


class TestPostgresEngineStreamToDestination:
    def test_passes_params_through(self, engine, mock_conn):
        batch_cursor = MagicMock()
        batch_cursor.description = [("id",)]
        batch_cursor.fetchmany.side_effect = [[(1,), (2,)], []]
        mock_conn.cursor.return_value = batch_cursor

        collected = []
        total = engine.stream_to_destination(
            "SELECT id FROM t WHERE x = %s",
            process_batch=lambda b: collected.extend(b),
            params=("val",),
            batch_size=100,
        )
        assert total == 2
        assert len(collected) == 2
        batch_cursor.execute.assert_called_once_with(
            "SELECT id FROM t WHERE x = %s",
            ("val",),
        )


class TestPostgresEngineIngestBatchConflict:
    def _get_executed_sql(self, mock_cursor) -> list[str]:
        return [
            call_args[0][0]
            for call_args in mock_cursor.execute.call_args_list
            if call_args[0]
        ]

    def test_empty_rows_returns_zero(self, engine):
        assert engine.ingest_batch([], "test", "raw") == 0

    def test_no_conflict_column(self, engine, mock_cursor):
        rows = [{"id": 1, "name": "alice"}]
        engine.ingest_batch(rows, "test", "raw")
        insert_stmts = _find_sql_containing(mock_cursor, "insert into")
        assert len(insert_stmts) == 1
        assert "on conflict" not in insert_stmts[0]

    def test_single_conflict_column_string_do_nothing(self, engine, mock_cursor):
        rows = [{"id": 1, "name": "alice", "age": 30}]
        engine.ingest_batch(
            rows, "test", "raw", conflict_column="id", conflict_action="NOTHING"
        )
        insert_stmts = _find_sql_containing(mock_cursor, "insert into")
        assert len(insert_stmts) == 1
        assert 'on conflict ("id") do nothing' in insert_stmts[0]

    def test_single_conflict_column_string_do_update(self, engine, mock_cursor):
        rows = [{"id": 1, "name": "alice", "age": 30}]
        engine.ingest_batch(
            rows, "test", "raw", conflict_column="id", conflict_action="UPDATE"
        )
        insert_stmts = _find_sql_containing(mock_cursor, "insert into")
        assert len(insert_stmts) == 1
        insert_sql = insert_stmts[0]
        assert 'on conflict ("id") do update set' in insert_sql
        assert '"name" = excluded."name"' in insert_sql
        assert '"age" = excluded."age"' in insert_sql
        assert '"id" = excluded."id"' not in insert_sql

    def test_composite_conflict_key_list(self, engine, mock_cursor):
        rows = [{"pin14": "123", "tax_year": 2024, "assessed_value": 50000}]
        engine.ingest_batch(
            rows,
            "assessments",
            "raw",
            conflict_column=["pin14", "tax_year"],
            conflict_action="UPDATE",
        )
        insert_stmts = _find_sql_containing(mock_cursor, "insert into")
        assert len(insert_stmts) == 1
        insert_sql = insert_stmts[0]
        assert 'on conflict ("pin14", "tax_year") do update set' in insert_sql
        assert '"assessed_value" = excluded."assessed_value"' in insert_sql
        assert '"pin14" = excluded."pin14"' not in insert_sql
        assert '"tax_year" = excluded."tax_year"' not in insert_sql

    def test_composite_conflict_key_do_nothing(self, engine, mock_cursor):
        rows = [{"a": 1, "b": 2, "c": 3}]
        engine.ingest_batch(
            rows, "test", "raw", conflict_column=["a", "b"], conflict_action="NOTHING"
        )
        insert_stmts = _find_sql_containing(mock_cursor, "insert into")
        assert len(insert_stmts) == 1
        assert 'on conflict ("a", "b") do nothing' in insert_stmts[0]

    def test_copy_writes_correct_tsv(self, engine, mock_cursor):
        rows = [
            {"id": 1, "name": "alice", "note": None},
            {"id": 2, "name": "bob\ttab", "note": "line\nnewline"},
        ]
        captured_buf = []
        original_copy = mock_cursor.copy_expert

        def capture_copy(sql, buf):
            captured_buf.append(buf.read())
            return original_copy(sql, buf)

        mock_cursor.copy_expert = capture_copy
        engine.ingest_batch(rows, "test", "raw")

        tsv = captured_buf[0]
        lines = tsv.strip().split("\n")
        assert len(lines) == 2
        assert "\\N" in lines[0]
        assert "\t" not in lines[1].split("\t")[1].replace("bob tab", "")

    def test_returns_rowcount(self, engine, mock_cursor):
        mock_cursor.rowcount = 5
        rows = [{"id": i} for i in range(5)]
        assert engine.ingest_batch(rows, "test", "raw") == 5


class TestStagedIngest:
    def test_write_batch_creates_staging_and_copies(self, engine, mock_cursor):
        with engine.staged_ingest("crimes", "raw_data") as stager:
            count = stager.write_batch([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])

            assert count == 2
            assert stager.rows_staged == 2
            assert stager._created is True

            # Staging table should be named after target
            assert "crimes" in stager._staging_table
            assert stager._staging_table.startswith("_staging_crimes_")

            # CREATE TEMP TABLE should reference the target
            create_stmts = _find_sql_containing(mock_cursor, "create temp table")
            assert any(stager._staging_table in s for s in create_stmts)

            # COPY should have been called
            copy_calls = [c for c in mock_cursor.method_calls if c[0] == "copy_expert"]
            assert len(copy_calls) == 1

    def test_multiple_batches_accumulate(self, engine, mock_cursor):
        with engine.staged_ingest("t", "s") as stager:
            stager.write_batch([{"id": 1}])
            stager.write_batch([{"id": 2}, {"id": 3}])

            assert stager.rows_staged == 3

            copy_calls = [c for c in mock_cursor.method_calls if c[0] == "copy_expert"]
            assert len(copy_calls) == 2

    def test_empty_batch_is_noop(self, engine, mock_cursor):
        with engine.staged_ingest("t", "s") as stager:
            count = stager.write_batch([])
            assert count == 0
            assert stager.rows_staged == 0
            assert not stager._created

    def test_merge_on_clean_exit(self, engine, mock_cursor):
        mock_cursor.rowcount = 5

        with engine.staged_ingest(
            "crimes",
            "raw_data",
            conflict_column="case_number",
            conflict_action="NOTHING",
        ) as stager:
            stager.write_batch([{"case_number": "C1", "val": "x"}])

        # After exit, merge should have run
        insert_stmts = _find_sql_containing(mock_cursor, "insert into raw_data.crimes")
        assert len(insert_stmts) == 1
        assert 'on conflict ("case_number") do nothing' in insert_stmts[0]
        assert stager.rows_merged == 5

    def test_merge_with_upsert(self, engine, mock_cursor):
        mock_cursor.rowcount = 3

        with engine.staged_ingest(
            "t",
            "s",
            conflict_column=["k1", "k2"],
            conflict_action="UPDATE",
        ) as stager:
            stager.write_batch([{"k1": 1, "k2": 2, "val": "a"}])

        insert_stmts = _find_sql_containing(mock_cursor, "insert into s.t")
        assert len(insert_stmts) == 1
        insert_sql = insert_stmts[0]
        assert 'on conflict ("k1", "k2") do update set' in insert_sql
        assert '"val" = excluded."val"' in insert_sql
        assert '"k1" = excluded."k1"' not in insert_sql

    def test_no_merge_on_error(self, engine, mock_cursor):
        with pytest.raises(RuntimeError, match="boom"):
            with engine.staged_ingest("t", "s") as stager:
                stager.write_batch([{"id": 1}])
                raise RuntimeError("boom")

        # No INSERT INTO should have been executed (merge skipped)
        insert_stmts = _find_sql_containing(mock_cursor, "insert into s.t")
        assert len(insert_stmts) == 0

        # But staging table should still be dropped
        drop_stmts = _find_sql_containing(mock_cursor, "drop table")
        assert any(stager._staging_table in s for s in drop_stmts)

    def test_drop_staging_on_clean_exit(self, engine, mock_cursor):
        with engine.staged_ingest("t", "s") as stager:
            stager.write_batch([{"id": 1}])

        drop_stmts = _find_sql_containing(mock_cursor, "drop table")
        assert any(stager._staging_table in s for s in drop_stmts)

    def test_no_staging_created_means_no_drop(self, engine, mock_cursor):
        """If no batches are written, no staging table exists to drop."""
        with engine.staged_ingest("t", "s") as stager:
            pass

        drop_stmts = _find_sql_containing(mock_cursor, "drop table")
        assert len(drop_stmts) == 0

    def test_staging_table_name_unique_per_call(self, engine):
        s1 = engine.staged_ingest("t", "s")
        s2 = engine.staged_ingest("t", "s")
        assert s1._staging_table != s2._staging_table

    def test_no_merge_when_zero_rows_staged(self, engine, mock_cursor):
        """If only empty batches are written, skip merge."""
        with engine.staged_ingest("t", "s") as stager:
            stager.write_batch([])

        insert_stmts = _find_sql_containing(mock_cursor, "insert into")
        assert len(insert_stmts) == 0


class TestPostgresEngineConnectionManagement:
    @patch("psycopg2.connect")
    def test_lazy_connection(self, mock_connect):
        mock_connect.return_value = MagicMock(closed=False)
        eng = PostgresEngine(
            DatabaseCredentials(
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            )
        )
        assert eng._conn is None
        _ = eng.connection
        mock_connect.assert_called_once()

    @patch("psycopg2.connect")
    def test_reconnects_if_closed(self, mock_connect):
        conn_old = MagicMock(closed=True)
        conn_new = MagicMock(closed=False)
        mock_connect.return_value = conn_new

        eng = PostgresEngine(
            DatabaseCredentials(
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            )
        )
        eng._conn = conn_old
        result = eng.connection
        mock_connect.assert_called_once()
        assert result == conn_new

    def test_context_manager_closes(self, mock_conn, sample_creds):
        eng = PostgresEngine(sample_creds)
        eng._conn = mock_conn
        with eng:
            pass
        mock_conn.close.assert_called_once()

    def test_close_idempotent(self, mock_conn, sample_creds):
        eng = PostgresEngine(sample_creds)
        eng._conn = mock_conn
        eng.close()
        eng.close()
        mock_conn.close.assert_called_once()

    def test_db_name_defaults_to_creds_database(self, sample_creds):
        eng = PostgresEngine(sample_creds)
        assert eng.db_name == "chicago"

    def test_db_name_override(self, sample_creds):
        eng = PostgresEngine(sample_creds, db_name="staging")
        assert eng.db_name == "staging"


@pytest.fixture
def sample_geojson(tmp_path):
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": 1, "name": "Park A"},
                "geometry": {"type": "Point", "coordinates": [-87.6298, 41.8781]},
            },
            {
                "type": "Feature",
                "properties": {"id": 2, "name": "Park B"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-87.63, 41.87],
                            [-87.62, 41.87],
                            [-87.62, 41.88],
                            [-87.63, 41.88],
                            [-87.63, 41.87],
                        ]
                    ],
                },
            },
            {
                "type": "Feature",
                "properties": {"id": 3, "name": "Park C"},
                "geometry": {"type": "Point", "coordinates": [-87.65, 41.90]},
            },
        ],
    }
    path = tmp_path / "test.geojson"
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def geojson_with_nulls(tmp_path):
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": 1, "name": "Valid"},
                "geometry": {"type": "Point", "coordinates": [-87.6, 41.8]},
            },
            {
                "type": "Feature",
                "properties": {"id": 2, "name": "No Geom"},
                "geometry": None,
            },
            {
                "type": "Feature",
                "properties": {"id": 3, "name": None},
                "geometry": {"type": "Point", "coordinates": [-87.7, 41.9]},
            },
        ],
    }
    path = tmp_path / "nulls.geojson"
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def empty_geojson(tmp_path):
    data = {"type": "FeatureCollection", "features": []}
    path = tmp_path / "empty.geojson"
    path.write_text(json.dumps(data))
    return path


def _engine_with_geometry(mock_conn, sample_creds, mock_cursor, geom_col="geom"):
    """
    Build an engine whose _get_geometry_info is pre-populated so that
    ingest_geojson doesn't need a real geometry_columns lookup.
    """
    eng = PostgresEngine(sample_creds)
    eng._conn = mock_conn
    # Pre-cache so _get_geometry_column doesn't trigger a real query
    eng._geometry_info_cache[("public", "parks")] = {geom_col: 4326}
    eng._geometry_info_cache[("public", "t")] = {geom_col: 4326}
    return eng


class TestIngestGeojson:
    @pytest.fixture(autouse=True)
    def setup_engine(self, mock_conn, sample_creds, mock_cursor):
        self.engine = _engine_with_geometry(mock_conn, sample_creds, mock_cursor)
        self.mock_cursor = mock_cursor

    def test_basic_ingest(self, sample_geojson):
        self.mock_cursor.rowcount = 3

        inserted, failed = self.engine.ingest_geojson(
            filepath=sample_geojson, target_table="parks", target_schema="public"
        )

        assert inserted == 3
        assert len(failed) == 0

        create_stmts = _find_sql_containing(self.mock_cursor, "create temp table")
        assert any("_geojson_staging" in s for s in create_stmts)

        copy_calls = [c for c in self.mock_cursor.method_calls if c[0] == "copy_expert"]
        assert len(copy_calls) >= 1

        assert len(_find_sql_containing(self.mock_cursor, "alter table")) >= 1
        assert (
            len(_find_sql_containing(self.mock_cursor, "insert into public.parks")) >= 1
        )

    def test_empty_file_returns_zero(self, empty_geojson):
        inserted, failed = self.engine.ingest_geojson(
            filepath=empty_geojson, target_table="parks", target_schema="public"
        )
        assert inserted == 0
        assert failed == []

    def test_null_geometry_logged_as_failure(self, geojson_with_nulls):
        self.mock_cursor.rowcount = 2

        inserted, failed = self.engine.ingest_geojson(
            filepath=geojson_with_nulls, target_table="parks", target_schema="public"
        )

        assert len(failed) == 1
        assert failed[0]["index"] == 1
        assert "no geometry" in failed[0]["error"].lower()

    def test_null_property_written_as_backslash_n(self, geojson_with_nulls):
        copied_data = []

        def capture_copy(sql, buf):
            copied_data.append(buf.read())

        self.mock_cursor.copy_expert.side_effect = capture_copy

        self.engine.ingest_geojson(
            filepath=geojson_with_nulls, target_table="parks", target_schema="public"
        )

        combined = "".join(copied_data)
        lines = [line for line in combined.strip().split("\n") if line]
        line_for_id3 = [line for line in lines if line.startswith("3\t")][0]
        fields = line_for_id3.split("\t")
        assert fields[1] == "\\N"

    def test_batching(self, sample_geojson):
        self.mock_cursor.rowcount = 3

        self.engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="parks",
            target_schema="public",
            batch_size=2,
        )

        copy_calls = [c for c in self.mock_cursor.method_calls if c[0] == "copy_expert"]
        assert len(copy_calls) == 2

    def test_conflict_do_nothing(self, sample_geojson):
        self.engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="parks",
            target_schema="public",
            conflict_column="id",
            conflict_action="NOTHING",
        )
        insert_stmts = [
            s
            for s in _find_sql_containing(self.mock_cursor, "insert into")
            if "on conflict" in s
        ]
        assert len(insert_stmts) == 1
        assert 'on conflict ("id") do nothing' in insert_stmts[0]

    def test_conflict_do_update(self, sample_geojson):
        self.engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="parks",
            target_schema="public",
            conflict_column="id",
            conflict_action="UPDATE",
        )
        insert_stmts = [
            s
            for s in _find_sql_containing(self.mock_cursor, "insert into")
            if "do update set" in s
        ]
        assert len(insert_stmts) == 1
        set_part = insert_stmts[0].split("do update set")[1]
        assert '"id"' not in set_part

    def test_composite_conflict_columns(self, sample_geojson):
        self.engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="parks",
            target_schema="public",
            conflict_column=["id", "name"],
            conflict_action="NOTHING",
        )
        insert_stmts = [
            s for s in _find_sql_containing(self.mock_cursor, "on conflict")
        ]
        assert any('"id", "name"' in s for s in insert_stmts)

    def test_copy_data_contains_wkb_hex(self, sample_geojson):
        copied_data = []

        def capture_copy(sql, buf):
            copied_data.append(buf.read())

        self.mock_cursor.copy_expert.side_effect = capture_copy

        self.engine.ingest_geojson(
            filepath=sample_geojson, target_table="parks", target_schema="public"
        )

        combined = "".join(copied_data)
        lines = combined.strip().split("\n")
        for line in lines:
            geom_field = line.split("\t")[-1]
            assert all(c in "0123456789abcdefABCDEF" for c in geom_field)

    def test_special_characters_escaped(self, tmp_path):
        copied_data = []

        def capture_copy(sql, buf):
            copied_data.append(buf.read())

        self.mock_cursor.copy_expert.side_effect = capture_copy

        data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"id": 1, "name": "Has\ttab\nand\\backslash"},
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                }
            ],
        }
        path = tmp_path / "special.geojson"
        path.write_text(json.dumps(data))

        self.engine.ingest_geojson(
            filepath=path, target_table="t", target_schema="public"
        )

        combined = "".join(copied_data)
        name_field = combined.strip().split("\t")[1]
        assert "\t" not in name_field
        assert "\n" not in name_field
        assert "\\\\" in combined

    def test_all_features_fail_returns_zero(self, tmp_path):
        data = {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "properties": {"id": 1}, "geometry": None},
                {"type": "Feature", "properties": {"id": 2}, "geometry": None},
            ],
        }
        path = tmp_path / "allfail.geojson"
        path.write_text(json.dumps(data))

        inserted, failed = self.engine.ingest_geojson(
            filepath=path, target_table="t", target_schema="public"
        )

        assert inserted == 0
        assert len(failed) == 2
