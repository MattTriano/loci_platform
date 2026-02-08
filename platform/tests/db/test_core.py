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
        mock_cursor.execute.side_effect = [
            psycopg2.OperationalError("connection reset"),
            None,
        ]
        engine.query("SELECT 1")
        assert mock_cursor.execute.call_count == 2

    def test_retries_on_interface_error(self, engine, mock_conn, mock_cursor):
        mock_cursor.execute.side_effect = [
            psycopg2.InterfaceError("connection closed"),
            None,
        ]
        engine.query("SELECT 1")
        assert mock_cursor.execute.call_count == 2

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
        engine.query(
            "SELECT * FROM log WHERE source = %(source)s AND dataset = %(dataset)s",
            params=params,
        )
        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM log WHERE source = %(source)s AND dataset = %(dataset)s",
            params,
        )

    def test_query_with_tuple_params(self, engine, mock_cursor):
        engine.query("SELECT * FROM log WHERE source = %s", params=("socrata",))
        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM log WHERE source = %s",
            ("socrata",),
        )

    def test_query_without_params(self, engine, mock_cursor):
        engine.query("SELECT 1")
        mock_cursor.execute.assert_called_once_with("SELECT 1", None)

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
        assert engine.ingest_batch([], "raw.test") == 0

    def test_no_conflict_column(self, engine, mock_cursor):
        rows = [{"id": 1, "name": "alice"}]
        engine.ingest_batch(rows, "raw.test")
        sqls = self._get_executed_sql(mock_cursor)
        insert_sql = [s for s in sqls if "INSERT INTO" in s][0]
        assert "ON CONFLICT" not in insert_sql

    def test_single_conflict_column_string_do_nothing(self, engine, mock_cursor):
        rows = [{"id": 1, "name": "alice", "age": 30}]
        engine.ingest_batch(
            rows, "raw.test", conflict_column="id", conflict_action="NOTHING"
        )
        sqls = self._get_executed_sql(mock_cursor)
        insert_sql = [s for s in sqls if "INSERT INTO" in s][0]
        assert 'ON CONFLICT ("id") DO NOTHING' in insert_sql

    def test_single_conflict_column_string_do_update(self, engine, mock_cursor):
        rows = [{"id": 1, "name": "alice", "age": 30}]
        engine.ingest_batch(
            rows, "raw.test", conflict_column="id", conflict_action="UPDATE"
        )
        sqls = self._get_executed_sql(mock_cursor)
        insert_sql = [s for s in sqls if "INSERT INTO" in s][0]
        assert 'ON CONFLICT ("id") DO UPDATE SET' in insert_sql
        assert '"name" = EXCLUDED."name"' in insert_sql
        assert '"age" = EXCLUDED."age"' in insert_sql
        assert '"id" = EXCLUDED."id"' not in insert_sql

    def test_composite_conflict_key_list(self, engine, mock_cursor):
        rows = [{"pin14": "123", "tax_year": 2024, "assessed_value": 50000}]
        engine.ingest_batch(
            rows,
            "raw.assessments",
            conflict_column=["pin14", "tax_year"],
            conflict_action="UPDATE",
        )
        sqls = self._get_executed_sql(mock_cursor)
        insert_sql = [s for s in sqls if "INSERT INTO" in s][0]
        assert 'ON CONFLICT ("pin14", "tax_year") DO UPDATE SET' in insert_sql
        assert '"assessed_value" = EXCLUDED."assessed_value"' in insert_sql
        assert '"pin14" = EXCLUDED."pin14"' not in insert_sql
        assert '"tax_year" = EXCLUDED."tax_year"' not in insert_sql

    def test_composite_conflict_key_do_nothing(self, engine, mock_cursor):
        rows = [{"a": 1, "b": 2, "c": 3}]
        engine.ingest_batch(
            rows, "raw.test", conflict_column=["a", "b"], conflict_action="NOTHING"
        )
        sqls = self._get_executed_sql(mock_cursor)
        insert_sql = [s for s in sqls if "INSERT INTO" in s][0]
        assert 'ON CONFLICT ("a", "b") DO NOTHING' in insert_sql

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
        engine.ingest_batch(rows, "raw.test")

        tsv = captured_buf[0]
        lines = tsv.strip().split("\n")
        assert len(lines) == 2
        assert "\\N" in lines[0]
        assert "\t" not in lines[1].split("\t")[1].replace("bob tab", "")

    def test_returns_rowcount(self, engine, mock_cursor):
        mock_cursor.rowcount = 5
        rows = [{"id": i} for i in range(5)]
        assert engine.ingest_batch(rows, "raw.test") == 5


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


class TestIngestGeojson:
    def test_basic_ingest(self, sample_geojson, engine, mock_cursor):
        mock_cursor.rowcount = 3

        inserted, failed = engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="public.parks",
        )

        assert inserted == 3
        assert len(failed) == 0

        # Extract all SQL strings sent to .execute() for easier searching
        all_execute_sql = [
            str(call[0][0]) for call in mock_cursor.execute.call_args_list
        ]

        # 1. Verify the Metadata Lookup (The call that caused the original failure)
        assert any("FROM geometry_columns" in sql for sql in all_execute_sql)

        # 2. Verify Staging Table Creation
        create_stmt = next(
            (sql for sql in all_execute_sql if "CREATE TEMP TABLE" in sql), None
        )
        assert create_stmt is not None, "Staging table was not created"
        assert "_geojson_staging" in create_stmt
        assert "public.parks" in create_stmt

        # 3. Verify Data Loading (copy_expert)
        copy_calls = [c for c in mock_cursor.method_calls if c[0] == "copy_expert"]
        assert len(copy_calls) == 1

        # 4. Verify Geometry Column Alteration
        assert any("ALTER TABLE" in sql for sql in all_execute_sql)

        # 5. Verify Final Insertion
        assert any("INSERT INTO public.parks" in sql for sql in all_execute_sql)

    def test_empty_file_returns_zero(self, empty_geojson, engine, mock_cursor):
        inserted, failed = engine.ingest_geojson(
            filepath=empty_geojson,
            target_table="public.parks",
        )

        assert inserted == 0
        assert failed == []

    def test_null_geometry_logged_as_failure(
        self, geojson_with_nulls, engine, mock_cursor
    ):
        mock_cursor.rowcount = 2

        inserted, failed = engine.ingest_geojson(
            filepath=geojson_with_nulls,
            target_table="public.parks",
        )

        assert len(failed) == 1
        assert failed[0]["index"] == 1
        assert "no geometry" in failed[0]["error"].lower()

    def test_null_property_written_as_backslash_n(
        self, geojson_with_nulls, engine, mock_cursor
    ):
        copied_data = []

        def capture_copy(sql, buf):
            copied_data.append(buf.read())

        mock_cursor.copy_expert.side_effect = capture_copy

        engine.ingest_geojson(
            filepath=geojson_with_nulls,
            target_table="public.parks",
        )

        combined = "".join(copied_data)
        lines = [l for l in combined.strip().split("\n") if l]
        line_for_id3 = [l for l in lines if l.startswith("3\t")][0]
        fields = line_for_id3.split("\t")
        assert fields[1] == "\\N"

    def test_batching(self, sample_geojson, engine, mock_cursor):
        mock_cursor.rowcount = 3

        engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="public.parks",
            batch_size=2,
        )

        copy_calls = [c for c in mock_cursor.method_calls if c[0] == "copy_expert"]
        assert len(copy_calls) == 2

    def test_conflict_do_nothing(self, sample_geojson, engine, mock_cursor):
        engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="public.parks",
            conflict_column="id",
            conflict_action="NOTHING",
        )

        insert_sql = [
            c[0][0]
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO" in str(c) and "ON CONFLICT" in str(c)
        ]
        assert len(insert_sql) == 1
        assert 'ON CONFLICT ("id") DO NOTHING' in insert_sql[0]

    def test_conflict_do_update(self, sample_geojson, engine, mock_cursor):
        engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="public.parks",
            conflict_column="id",
            conflict_action="UPDATE",
        )

        insert_sql = [
            c[0][0]
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO" in str(c) and "DO UPDATE SET" in str(c)
        ]
        assert len(insert_sql) == 1
        assert 'ON CONFLICT ("id") DO UPDATE SET' in insert_sql[0]
        set_part = insert_sql[0].split("DO UPDATE SET")[1]
        assert '"id"' not in set_part

    def test_composite_conflict_columns(self, sample_geojson, engine, mock_cursor):
        engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="public.parks",
            conflict_column=["id", "name"],
            conflict_action="NOTHING",
        )

        insert_sql = [
            c[0][0]
            for c in mock_cursor.execute.call_args_list
            if "ON CONFLICT" in str(c)
        ]
        assert '"id", "name"' in insert_sql[0]

    def test_copy_data_contains_wkb_hex(self, sample_geojson, engine, mock_cursor):
        copied_data = []

        def capture_copy(sql, buf):
            copied_data.append(buf.read())

        mock_cursor.copy_expert.side_effect = capture_copy

        engine.ingest_geojson(
            filepath=sample_geojson,
            target_table="public.parks",
        )

        combined = "".join(copied_data)
        lines = combined.strip().split("\n")
        for line in lines:
            geom_field = line.split("\t")[-1]
            assert all(c in "0123456789abcdefABCDEF" for c in geom_field)

    def test_special_characters_escaped(self, tmp_path, engine, mock_cursor):
        copied_data = []

        def capture_copy(sql, buf):
            copied_data.append(buf.read())

        mock_cursor.copy_expert.side_effect = capture_copy

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

        engine.ingest_geojson(filepath=path, target_table="public.t")

        combined = "".join(copied_data)
        name_field = combined.strip().split("\t")[1]
        assert "\t" not in name_field
        assert "\n" not in name_field
        assert "\\\\" in combined

    def test_all_features_fail_returns_zero(self, tmp_path, engine, mock_cursor):
        data = {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "properties": {"id": 1}, "geometry": None},
                {"type": "Feature", "properties": {"id": 2}, "geometry": None},
            ],
        }
        path = tmp_path / "allfail.geojson"
        path.write_text(json.dumps(data))

        inserted, failed = engine.ingest_geojson(
            filepath=path,
            target_table="public.t",
        )

        assert inserted == 0
        assert len(failed) == 2
