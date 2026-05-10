from unittest.mock import MagicMock, PropertyMock, patch
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
import pytest
from loci.db.core import DatabaseCredentials, PostgresEngine


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
    cur.description = [
        ("col_a", 23, None, None, None, None, None),
        ("col_b", 25, None, None, None, None, None),
    ]
    cur.fetchone.return_value = None
    cur.rowcount = 2
    cur.close.return_value = None

    # Default fetchall: return empty so _get_target_columns returns []
    # Tests that need specific columns should override via _set_table_columns helper
    cur.fetchall.return_value = []

    return cur


def _set_table_columns(mock_cursor, columns: list[str]):
    """Configure the mock to return these columns from _get_target_columns."""
    mock_cursor.fetchall.return_value = [(c,) for c in columns]


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
        str(call_args[0][0]) for call_args in mock_cursor.execute.call_args_list if call_args[0]
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
        assert creds.connection_string == "postgresql://admin:plainpass@localhost:5432/mydb"

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
        main_query_calls = [c for c in mock_cursor.execute.call_args_list if c[0][0] == "SELECT 1"]
        assert len(main_query_calls) == 2

    def test_retries_on_interface_error(self, engine, mock_conn, mock_cursor):
        mock_cursor.execute.side_effect = [
            psycopg2.InterfaceError("connection closed"),
            None,
            None,
        ]
        engine.query("SELECT 1")
        main_query_calls = [c for c in mock_cursor.execute.call_args_list if c[0][0] == "SELECT 1"]
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
        matching = [c for c in mock_cursor.execute.call_args_list if c[0][0] == "SELECT 1"]
        assert len(matching) == 1

    def test_query_returns_dataframe(self, engine, mock_cursor):
        mock_cursor.description = [
            ("id", 23, None, None, None, None, None),
            ("name", 25, None, None, None, None, None),
        ]
        mock_cursor.fetchall.return_value = [(1, "alice"), (2, "bob")]
        df = engine.query("SELECT id, name FROM users WHERE active = %s", params=(True,))
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


class TestStagedIngest:
    def test_write_batch_creates_staging_and_copies(self, engine, mock_cursor):
        with engine.staged_ingest("crimes", "raw_data") as stager:
            count = stager.write_batch([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])

            assert count == 2
            assert stager.rows_staged == 2
            assert stager._created is True

            # Staging table should be named after target
            assert "crimes" in stager._staging_table
            assert stager._staging_table.startswith("_staging_crimes")

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
        _set_table_columns(mock_cursor, ["k1", "k2", "val"])

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

    def test_merges_partial_data_on_error(self, engine, mock_cursor):
        """On error, staged rows are still merged before re-raising."""
        mock_cursor.rowcount = 1

        with pytest.raises(RuntimeError, match="boom"):
            with engine.staged_ingest("t", "s") as stager:
                stager.write_batch([{"id": 1}])
                raise RuntimeError("boom")

        # Merge SHOULD have happened with the partial data
        insert_stmts = _find_sql_containing(mock_cursor, "insert into s.t")
        assert len(insert_stmts) == 1
        assert stager.rows_merged == 1

        # Staging table should still be dropped
        drop_stmts = _find_sql_containing(mock_cursor, "drop table")
        assert any(stager._staging_table in s for s in drop_stmts)

    def test_drop_staging_on_clean_exit(self, engine, mock_cursor):
        with engine.staged_ingest("t", "s") as stager:
            stager.write_batch([{"id": 1}])

        drop_stmts = _find_sql_containing(mock_cursor, "drop table")
        assert any(stager._staging_table in s for s in drop_stmts)

    def test_no_staging_created_means_no_drop(self, engine, mock_cursor):
        """If no batches are written, no staging table exists to drop."""
        with engine.staged_ingest("t", "s") as stager:  #  noqa F841
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


class TestGeometryDetectionByOid:
    """Regression tests for OID-based geometry detection.

    The original name-based detection (`_detect_geometry_in_result`) matched
    column names against the PostGIS `geometry_columns` catalog. This caused
    false positives when a non-geometry column (e.g. a text column called
    "location") shared a name with a geometry column on a different table.

    The OID-based approach checks `cursor.description[i][1]` (the Postgres
    type OID) instead, which is unambiguous.
    """

    GEOMETRY_OID = 16384  # typical PostGIS geometry type OID

    def _make_description(self, columns: list[tuple[str, int]]):
        """Build a psycopg2-style description from (name, oid) pairs."""
        return [(name, oid, None, None, None, None, None) for name, oid in columns]

    def test_text_column_named_location_not_detected_as_geometry(self, engine, mock_cursor):
        """Regression: a text column called 'location' must not be treated as geometry."""
        engine._geometry_oid = self.GEOMETRY_OID

        columns = ["id", "location", "description"]
        type_oids = [23, 25, 25]  # int4, text, text

        geom_col, srid = engine._detect_geometry_by_oid(columns, type_oids)

        assert geom_col is None
        assert srid == 0

    def test_actual_geometry_column_detected(self, engine, mock_cursor):
        """A column whose OID matches the geometry type should be detected."""
        engine._geometry_oid = self.GEOMETRY_OID

        columns = ["id", "geom", "name"]
        type_oids = [23, self.GEOMETRY_OID, 25]

        geom_col, srid = engine._detect_geometry_by_oid(columns, type_oids)

        assert geom_col == "geom"

    def test_geometry_column_uses_cached_srid(self, engine, mock_cursor):
        """When the geometry column appears in the SRID cache, use the cached SRID."""
        engine._geometry_oid = self.GEOMETRY_OID
        engine._geometry_info_cache[("public", "parcels")] = {"geom": 4326}

        columns = ["id", "geom"]
        type_oids = [23, self.GEOMETRY_OID]

        geom_col, srid = engine._detect_geometry_by_oid(columns, type_oids)

        assert geom_col == "geom"
        assert srid == 4326

    def test_geometry_column_defaults_to_srid_zero_when_not_cached(self, engine, mock_cursor):
        """Without a cache entry, SRID falls back to 0."""
        engine._geometry_oid = self.GEOMETRY_OID
        engine._geometry_info_cache.clear()

        columns = ["id", "the_geom"]
        type_oids = [23, self.GEOMETRY_OID]

        geom_col, srid = engine._detect_geometry_by_oid(columns, type_oids)

        assert geom_col == "the_geom"
        assert srid == 0

    def test_no_geometry_columns_returns_none(self, engine, mock_cursor):
        """A result set with no geometry columns returns (None, 0)."""
        engine._geometry_oid = self.GEOMETRY_OID

        columns = ["id", "name", "value"]
        type_oids = [23, 25, 23]

        geom_col, srid = engine._detect_geometry_by_oid(columns, type_oids)

        assert geom_col is None
        assert srid == 0

    def test_postgis_not_installed_returns_none(self, engine, mock_cursor):
        """If the geometry OID lookup failed (no PostGIS), detection is skipped."""
        engine._geometry_oid = None

        columns = ["id", "geom"]
        type_oids = [23, 99999]

        geom_col, srid = engine._detect_geometry_by_oid(columns, type_oids)

        assert geom_col is None
        assert srid == 0


def _setup_scd2_cursor(mock_cursor, columns, rowcounts):
    """Configure a mock cursor for an SCD2 merge.

    Args:
        columns: list of target column names (excluding metadata).
        rowcounts: dict with optional keys 'deduped', 'invalidated',
                   'closed', 'merged'. Defaults to 0 each.
    """
    _set_table_columns(mock_cursor, columns)

    rc_sequence = [
        2,  # write_batch's rowcount read (unused but cursor.rowcount may be read)
        rowcounts.get("deduped", 0),
        rowcounts.get("invalidated", 0),
        rowcounts.get("closed", 0),
        rowcounts.get("merged", 0),
    ]
    type(mock_cursor).rowcount = PropertyMock(side_effect=rc_sequence + [0] * 10)


class TestStagedIngestSCD2:
    """SCD Type 2 merge behavior."""

    def test_entity_key_and_conflict_column_together_raises(self, engine):
        with pytest.raises(ValueError, match="entity_key.*conflict_column"):
            engine.staged_ingest(
                "t",
                "s",
                entity_key=["id"],
                conflict_column=["id"],
            )

    def test_invalidate_missing_without_entity_key_raises(self, engine):
        with pytest.raises(ValueError, match="invalidate_missing requires entity_key"):
            engine.staged_ingest("t", "s", invalidate_missing=True)

    def test_scd2_merge_runs_when_entity_key_set(self, engine, mock_cursor):
        """An SCD2 merge runs the hash/close-out/insert sequence,
        not the simple `on conflict` insert."""
        _set_table_columns(mock_cursor, ["id", "val"])

        with engine.staged_ingest(
            "crimes",
            "raw_data",
            entity_key=["id"],
            metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
        ) as stager:
            stager.write_batch([{"id": 1, "val": "a"}])

        sqls = _get_execute_sql_strings(mock_cursor)

        # SCD2-specific statements should appear
        assert any('add column if not exists "record_hash"' in s for s in sqls)
        assert any('set "record_hash" = md5(' in s for s in sqls)
        assert any('set "valid_to" = now()' in s for s in sqls)

        # Insert should target (entity_key, record_hash) conflict, not a
        # simple conflict_column upsert
        insert_stmts = _find_sql_containing(mock_cursor, "insert into raw_data.crimes")
        assert len(insert_stmts) == 1
        assert '("id", "record_hash") do nothing' in insert_stmts[0]

    def test_scd2_dedupes_staging_against_target_history(self, engine, mock_cursor):
        """Regression: staging rows whose (entity_key, record_hash) already
        exists anywhere in target history must be deleted from staging
        before the close-out fires.

        Without this dedupe, a replayed search-shaped row on a second
        incremental run would close out the current detail-shaped row
        with no replacement (the insert no-ops on the unique constraint),
        leaving the entity with zero current versions. This was an
        observed data-loss bug.
        """
        _set_table_columns(mock_cursor, ["id", "val"])

        with engine.staged_ingest(
            "t",
            "s",
            entity_key=["id"],
            metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
        ) as stager:
            stager.write_batch([{"id": 1, "val": "a"}])

        sqls = _get_execute_sql_strings(mock_cursor)

        # The dedupe DELETE must run BEFORE the close-out UPDATE
        dedupe_idx = next(
            (i for i, s in enumerate(sqls) if "delete from" in s and stager._staging_table in s),
            None,
        )
        closeout_idx = next(
            (
                i
                for i, s in enumerate(sqls)
                if 'set "valid_to" = now()' in s and "raw_data" not in s
            ),
            next(
                (i for i, s in enumerate(sqls) if 'set "valid_to" = now()' in s),
                None,
            ),
        )

        assert dedupe_idx is not None, "dedupe DELETE was not emitted"
        assert closeout_idx is not None, "close-out UPDATE was not emitted"
        assert dedupe_idx < closeout_idx, (
            "dedupe DELETE must run before close-out UPDATE; "
            "otherwise a replayed historical hash will invalidate "
            "the current version with no replacement landing."
        )

        # The dedupe should join staging to target on the entity key
        dedupe_sql = sqls[dedupe_idx]
        assert '"id"' in dedupe_sql
        assert '"record_hash"' in dedupe_sql

    def test_scd2_close_out_compares_hash(self, engine, mock_cursor):
        """The close-out UPDATE should only invalidate current target rows
        whose record_hash differs from the staging row for the same entity."""
        _set_table_columns(mock_cursor, ["id", "val"])

        with engine.staged_ingest(
            "t",
            "s",
            entity_key=["id"],
            metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
        ) as stager:
            stager.write_batch([{"id": 1, "val": "a"}])

        update_stmts = [
            s for s in _get_execute_sql_strings(mock_cursor) if 'set "valid_to" = now()' in s
        ]
        # At least the close-out (and possibly invalidate_missing) UPDATE
        assert len(update_stmts) >= 1

        close_out = next(s for s in update_stmts if 'record_hash" != t."record_hash"' in s)
        assert 'where "valid_to" is null' in close_out

    def test_scd2_invalidate_missing_emits_extra_update(self, engine, mock_cursor):
        """With invalidate_missing=True, an additional UPDATE invalidates
        current target rows whose entity_key is absent from staging."""
        _set_table_columns(mock_cursor, ["id", "val"])

        with engine.staged_ingest(
            "t",
            "s",
            entity_key=["id"],
            metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
            invalidate_missing=True,
        ) as stager:
            stager.write_batch([{"id": 1, "val": "a"}])

        update_stmts = [
            s for s in _get_execute_sql_strings(mock_cursor) if 'set "valid_to" = now()' in s
        ]
        # Two UPDATEs: invalidate-missing + close-out-superseded
        assert len(update_stmts) == 2

        invalidate_missing = next(s for s in update_stmts if "not exists" in s)
        assert 'where "valid_to" is null' in invalidate_missing

    def test_scd2_invalidate_missing_default_off(self, engine, mock_cursor):
        """Without invalidate_missing, only the close-out UPDATE runs."""
        _set_table_columns(mock_cursor, ["id", "val"])

        with engine.staged_ingest(
            "t",
            "s",
            entity_key=["id"],
            metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
        ) as stager:
            stager.write_batch([{"id": 1, "val": "a"}])

        update_stmts = [
            s for s in _get_execute_sql_strings(mock_cursor) if 'set "valid_to" = now()' in s
        ]
        assert len(update_stmts) == 1
        assert "not exists" not in update_stmts[0]

    def test_scd2_hash_excludes_entity_key_and_metadata(self, engine, mock_cursor):
        """The MD5 hash must be computed over data columns only —
        not entity_key (which is the identity) and not metadata (which
        changes every run regardless of content)."""
        _set_table_columns(
            mock_cursor,
            ["id", "name", "val"],  # _get_target_columns already strips metadata
        )

        with engine.staged_ingest(
            "t",
            "s",
            entity_key=["id"],
            metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
        ) as stager:
            stager.write_batch([{"id": 1, "name": "x", "val": "a"}])

        hash_stmts = _find_sql_containing(mock_cursor, 'set "record_hash" = md5(')
        assert len(hash_stmts) == 1
        hash_sql = hash_stmts[0]

        # Data columns should appear in the hash expression
        assert '"name"' in hash_sql
        assert '"val"' in hash_sql

        # Entity key should NOT — it's the identity, not the content
        assert '"id"::text' not in hash_sql

        # Metadata columns should NOT — they're filtered before hashing
        for meta in ("ingested_at", "record_hash", "valid_from", "valid_to"):
            assert f'"{meta}"::text' not in hash_sql

    def test_scd2_composite_entity_key(self, engine, mock_cursor):
        """SCD2 with a multi-column entity_key joins on all of them."""
        _set_table_columns(mock_cursor, ["u", "v", "key", "weight"])

        with engine.staged_ingest(
            "edges",
            "raw_data",
            entity_key=["u", "v", "key"],
            metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
        ) as stager:
            stager.write_batch([{"u": 1, "v": 2, "key": 0, "weight": 3.0}])

        insert_stmts = _find_sql_containing(mock_cursor, "insert into raw_data.edges")
        assert len(insert_stmts) == 1
        assert '("u", "v", "key", "record_hash") do nothing' in insert_stmts[0]

        # Close-out should join on all three entity columns
        update_stmts = [
            s for s in _get_execute_sql_strings(mock_cursor) if 'set "valid_to" = now()' in s
        ]
        close_out = next(s for s in update_stmts if "record_hash" in s)
        assert 't."u" = s."u"' in close_out
        assert 't."v" = s."v"' in close_out
        assert 't."key" = s."key"' in close_out

    def test_scd2_no_data_columns_to_hash_raises(self, engine, mock_cursor):
        """If every non-entity_key column is excluded from hashing,
        there's nothing left to hash and the merge cannot proceed."""
        _set_table_columns(mock_cursor, ["id"])  # only the entity_key

        with pytest.raises(ValueError, match="No columns to hash"):
            with engine.staged_ingest(
                "t",
                "s",
                entity_key=["id"],
                metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
            ) as stager:
                stager.write_batch([{"id": 1}])

    def test_scd2_invalidate_missing_runs_before_dedupe(self, engine, mock_cursor):
        """Regression: invalidate_missing must check staging before dedupe
        removes unchanged-entity rows.

        If dedupe runs first, every unchanged entity gets dropped from
        staging (its (entity, hash) matches target history). Then
        invalidate_missing's `not exists in staging` check fires for every
        unchanged entity, closing out the entire current-version set.

        This was an observed data-loss bug in full-refresh collectors:
        a re-fetched graph identical to the previous run had every current
        edge invalidated, leaving only the small fraction that actually
        changed.
        """
        _set_table_columns(mock_cursor, ["id", "val"])

        with engine.staged_ingest(
            "edges",
            "raw_data",
            entity_key=["id"],
            metadata_columns={"ingested_at", "record_hash", "valid_from", "valid_to"},
            invalidate_missing=True,
        ) as stager:
            stager.write_batch([{"id": 1, "val": "a"}])

        sqls = _get_execute_sql_strings(mock_cursor)

        invalidate_idx = next(
            (i for i, s in enumerate(sqls) if 'set "valid_to" = now()' in s and "not exists" in s),
            None,
        )
        dedupe_idx = next(
            (i for i, s in enumerate(sqls) if "delete from" in s and stager._staging_table in s),
            None,
        )

        assert invalidate_idx is not None, "invalidate_missing UPDATE was not emitted"
        assert dedupe_idx is not None, "dedupe DELETE was not emitted"
        assert invalidate_idx < dedupe_idx, (
            "invalidate_missing must check staging BEFORE dedupe; "
            "otherwise unchanged entities are dropped from staging and "
            "then invalidated as 'missing'."
        )
