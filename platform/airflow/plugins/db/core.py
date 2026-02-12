import io
import uuid
import logging
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus
from typing import Any, Iterator, Optional

import geopandas as gpd
import pymysql
import pandas as pd
import psycopg2
import psycopg2.extras

from psycopg2.extensions import connection as Psycopg2Connection
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


def get_logger(
    name: str,
    level: int = logging.INFO,
    log_format: Optional[str] = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    log_format = log_format or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


@dataclass
class DatabaseCredentials:
    host: str
    port: int
    database: str
    username: str
    password: str
    driver: str = "postgresql"

    @classmethod
    def from_env_file(
        cls, env_path: str | Path, prefix: str, driver: str
    ) -> "DatabaseCredentials":
        """
        Load credentials from a .env file using variables matching a prefix pattern.

        Expected variables:
            prefix_HOST, prefix_PORT, prefix_DATABASE, prefix_USERNAME,
            prefix_PASSWORD, prefix_DRIVER (optional)
        """
        env_vars = cls._parse_env_file(env_path)

        def get_var(name: str, default: Optional[str] = None) -> str:
            key = f"{prefix}{name}"
            value = env_vars.get(key) or os.environ.get(key) or default
            if value is None:
                raise ValueError(f"Missing required environment variable: {key}")
            return value

        return cls(
            host=get_var("HOST"),
            port=int(get_var("PORT", "5432")),
            database=get_var("DATABASE"),
            username=get_var("USER"),
            password=get_var("PASSWORD"),
            driver=get_var("DRIVER", driver),
        )

    @staticmethod
    def _parse_env_file(env_path: str | Path) -> dict[str, str]:
        env_vars = {}
        path = Path(env_path)

        if not path.exists():
            return env_vars

        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
                if match:
                    key, value = match.groups()
                    value = value.strip()
                    if (value.startswith('"') and value.endswith('"')) or (
                        value.startswith("'") and value.endswith("'")
                    ):
                        value = value[1:-1]
                    env_vars[key] = value

        return env_vars

    @property
    def connection_string(self) -> str:
        encoded_password = quote_plus(self.password)
        return (
            f"{self.driver}://{self.username}:{encoded_password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def redacted_connection_string(self) -> str:
        return f"{self.driver}://{self.username}:****@****:{self.port}/{self.database}"

    def __str__(self) -> str:
        return (
            f"DatabaseCredentials(driver={self.driver!r}, "
            f"host='****', port={self.port}, database={self.database!r}, "
            f"username={self.username!r}, password='****')"
        )

    def __repr__(self) -> str:
        return self.__str__()


class StagedIngest:
    """
    Accumulates batches into a staging table via COPY, then merges into
    the target table on context-manager exit.

    Usage:
        with engine.staged_ingest(
            target_table="crimes",
            target_schema="raw_data",
            conflict_column=["case_number"],
            conflict_action="UPDATE",
        ) as stager:
            for batch in source.paginate(...):
                stager.write_batch(batch)

        print(stager.rows_staged, stager.rows_merged)
    """

    def __init__(
        self,
        engine: "PostgresEngine",
        target_table: str,
        target_schema: str,
        conflict_column: str | list[str] | None = None,
        conflict_action: str = "NOTHING",
    ) -> None:
        self._engine = engine
        self._target_table = target_table
        self._target_schema = target_schema
        self._fqn = f"{target_schema}.{target_table}"

        if isinstance(conflict_column, str):
            self._conflict_columns = [conflict_column]
        else:
            self._conflict_columns = conflict_column

        self._conflict_action = conflict_action

        # Use a short random suffix so parallel ingests don't collide
        suffix = uuid.uuid4().hex[:8]
        self._staging_table = f"_staging_{target_table}_{suffix}"

        self._columns: list[str] | None = None
        self._col_list: str | None = None
        self._created = False

        self.rows_staged = 0
        self.rows_merged = 0

        self._engine.logger.info(
            "StagedIngest: staging table %s for target %s",
            self._staging_table,
            self._fqn,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_batch(self, rows: list[dict[str, Any]]) -> int:
        """
        COPY a batch of rows into the staging table.

        The first call creates the staging table and locks in the column list.

        Returns the number of rows written.
        """
        if not rows:
            return 0

        rows = self._engine._normalize_json_values(rows)

        if not self._created:
            self._columns = list(rows[0].keys())
            self._col_list = ", ".join(f'"{c}"' for c in self._columns)
            self._create_staging_table()

        buf = self._rows_to_copy_buffer(rows)

        with self._engine.cursor() as cur:
            cur.copy_expert(
                f"copy {self._staging_table} ({self._col_list}) "
                f"from stdin with (format text, NULL '\\N')",
                buf,
            )

        count = len(rows)
        self.rows_staged += count
        return count

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "StagedIngest":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            if self.rows_staged > 0:
                self._cast_geometry_if_needed()
                self._merge()
                if exc_type is not None:
                    self._engine.logger.warning(
                        "StagedIngest: merged %d rows from %s despite error: %s",
                        self.rows_merged,
                        self._staging_table,
                        exc_val,
                    )
        except Exception as merge_err:
            self._engine.logger.error(
                "StagedIngest: merge failed for %s: %s",
                self._staging_table,
                merge_err,
            )
            # If the original exit was clean but merge itself failed,
            # let the merge error propagate
            if exc_type is None:
                raise
        finally:
            self._drop_staging_table()
        return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_staging_table(self) -> None:
        with self._engine.cursor() as cur:
            cur.execute(
                f"create temp table {self._staging_table} "
                f"(like {self._fqn} including defaults)"
            )
        self._created = True
        self._engine.logger.info("Created staging table %s", self._staging_table)

    def _drop_staging_table(self) -> None:
        if not self._created:
            return
        try:
            with self._engine.cursor() as cur:
                cur.execute(f"drop table if exists {self._staging_table}")
            self._engine.logger.info("Dropped staging table %s", self._staging_table)
        except Exception as e:
            self._engine.logger.warning(
                "Failed to drop staging table %s: %s", self._staging_table, e
            )

    def _rows_to_copy_buffer(self, rows: list[dict[str, Any]]) -> io.StringIO:
        buf = io.StringIO()
        for row in rows:
            vals = []
            for c in self._columns:
                v = row.get(c)
                if v is None:
                    vals.append("\\N")
                else:
                    vals.append(
                        str(v)
                        .replace("\\", "\\\\")
                        .replace("\t", " ")
                        .replace("\n", " ")
                    )
            buf.write("\t".join(vals) + "\n")
        buf.seek(0)
        return buf

    def _cast_geometry_if_needed(self) -> None:
        """If the target table has a PostGIS geometry column, cast it in staging."""
        try:
            geom_col = self._engine._get_geometry_column(
                self._target_table, self._target_schema
            )
        except ValueError:
            return  # no geometry column — nothing to do

        self._engine.logger.info(
            "Casting geometry column '%s' in staging table", geom_col
        )
        with self._engine.cursor() as cur:
            cur.execute(
                f"alter table {self._staging_table} "
                f'alter column "{geom_col}" type geometry '
                f'using "{geom_col}"::geometry'
            )

    def _merge(self) -> None:
        """INSERT from staging into target, with optional ON CONFLICT."""
        insert_sql = (
            f"insert into {self._fqn} ({self._col_list}) "
            f"select {self._col_list} from {self._staging_table}"
        )

        if self._conflict_columns:
            conflict_clause = ", ".join(f'"{c}"' for c in self._conflict_columns)

            if self._conflict_action.upper() == "UPDATE":
                update_cols = [
                    c for c in self._columns if c not in self._conflict_columns
                ]
                set_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
                insert_sql += (
                    f" on conflict ({conflict_clause}) do update set {set_clause}"
                )
            else:
                insert_sql += f" on conflict ({conflict_clause}) do nothing"

        with self._engine.cursor() as cur:
            cur.execute(insert_sql)
            self.rows_merged = cur.rowcount

        self._engine.logger.info(
            "Merged %d rows into %s (staged %d)",
            self.rows_merged,
            self._fqn,
            self.rows_staged,
        )


def pg_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(
            (psycopg2.OperationalError, psycopg2.InterfaceError)
        ),
        reraise=True,
    )


def mysql_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(
            (pymysql.OperationalError, pymysql.InterfaceError)
        ),
        reraise=True,
    )


class PostgresEngine:
    def __init__(
        self, creds: DatabaseCredentials, db_name: Optional[str] = None
    ) -> None:
        self.creds = creds
        self.db_name = db_name or creds.database
        self._conn: Optional[Psycopg2Connection] = None
        self.logger = get_logger("postgres_engine")
        self._geometry_info_cache: dict[tuple[str, str], dict[str, int]] = {}

    def _connect(self) -> Psycopg2Connection:
        return psycopg2.connect(
            host=self.creds.host,
            port=self.creds.port,
            dbname=self.db_name,
            user=self.creds.username,
            password=self.creds.password,
        )

    @contextmanager
    def transaction(self):
        try:
            yield self.connection
            self.connection.commit()
        except Exception as e:
            self.logger.error(f"Transaction failed with error {e}")
            self.connection.rollback()
            raise

    @contextmanager
    def cursor(self):
        with self.transaction():
            cur = self.connection.cursor()
            try:
                yield cur
            finally:
                cur.close()

    @property
    def connection(self) -> Psycopg2Connection:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def staged_ingest(
        self,
        target_table: str,
        target_schema: str,
        conflict_column: str | list[str] | None = None,
        conflict_action: str = "NOTHING",
    ) -> StagedIngest:
        """
        Return a StagedIngest context manager that accumulates batches
        into a staging table, then merges into the target on exit.

        Usage:
            with engine.staged_ingest("crimes", "raw_data",
                                       conflict_column=["case_number"],
                                       conflict_action="UPDATE") as stager:
                stager.write_batch(rows)

            print(stager.rows_staged, stager.rows_merged)
        """
        return StagedIngest(
            engine=self,
            target_table=target_table,
            target_schema=target_schema,
            conflict_column=conflict_column,
            conflict_action=conflict_action,
        )

    @pg_retry()
    def query(
        self,
        sql: str,
        params: dict[str, Any] | tuple | None = None,
    ) -> pd.DataFrame:
        """
        Execute a SELECT and return results as a DataFrame.

        If the result set includes a PostGIS geometry column, returns a
        GeoDataFrame with the geometry parsed and CRS set from the table's
        SRID. The geometry detection uses a cached lookup against the
        PostGIS geometry_columns catalog, so overhead on non-spatial
        queries is negligible after the first call per table.

        Args:
            sql:    SQL string. Use %(name)s for named params or %s for positional.
            params: Dict for named params, tuple for positional, or None.
        """
        with self.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(cur.fetchall(), columns=columns)

        if df.empty:
            return df

        geom_col, srid = self._detect_geometry_in_result(columns)
        if geom_col is None:
            return df

        return self._to_geodataframe(df, geom_col, srid)

    def _detect_geometry_in_result(self, columns: list[str]) -> tuple[str | None, int]:
        """
        Check whether any column in the result set is a known geometry column.

        Only matches against tables already present in _geometry_info_cache
        (populated by staged_ingest, ingest_geojson, or manual calls to
        _get_geometry_info). This avoids false positives from PostGIS
        extension functions like normalize_address whose output columns
        happen to share names with geometry_columns entries.

        Returns (geometry_column_name, srid) or (None, 0).
        """
        column_set = set(columns)
        for (_schema, _table), info in self._geometry_info_cache.items():
            for col_name, srid in info.items():
                if col_name in column_set:
                    return col_name, srid

        return None, 0

    @staticmethod
    def _to_geodataframe(
        df: pd.DataFrame, geom_col: str, srid: int
    ) -> gpd.GeoDataFrame:
        """Convert a DataFrame with a WKB hex geometry column to a GeoDataFrame."""
        from shapely import wkb

        df[geom_col] = df[geom_col].apply(
            lambda v: wkb.loads(v, hex=True) if v is not None else None
        )
        crs = f"EPSG:{srid}" if srid else None
        return gpd.GeoDataFrame(df, geometry=geom_col, crs=crs)

    @pg_retry()
    def execute(
        self,
        sql: str,
        params: dict[str, Any] | tuple | None = None,
    ) -> None:
        """
        Execute a DDL/DML statement (no result set).

        Args:
            sql:    SQL string. Use %(name)s for named params or %s for positional.
            params: Dict for named params, tuple for positional, or None.
        """
        try:
            with self.cursor() as cur:
                cur.execute(sql, params)
        except Exception as e:
            self.logger.error(f"Command failed with error {e}")
            raise

    @pg_retry()
    def query_batches(
        self,
        sql: str,
        params: dict[str, Any] | tuple | None = None,
        batch_size: int = 10000,
        as_dicts: bool = True,
    ) -> Iterator[list[dict] | pd.DataFrame]:
        """
        Server-side cursor for large result sets, yielded in batches.

        Args:
            sql:        SQL string with optional parameter placeholders.
            params:     Dict for named params, tuple for positional, or None.
            batch_size: Rows per batch.
            as_dicts:   If True, yield list[dict]; otherwise yield DataFrames.
        """
        cursor = self.connection.cursor(name="batch_cursor")
        cursor.itersize = batch_size
        try:
            cursor.execute(sql, params)
            columns = None

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                if columns is None:
                    columns = [desc[0] for desc in cursor.description]

                if as_dicts:
                    yield [dict(zip(columns, row)) for row in rows]
                else:
                    yield pd.DataFrame(rows, columns=columns)
        except Exception as e:
            self.logger.error(f"Query batches failed: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def _get_geometry_info(
        self, target_table: str, target_schema: str
    ) -> dict[str, int]:
        """
        Return a dict of {geometry_column_name: srid} for the given table.

        Results are cached per (schema, table) for the lifetime of the engine.
        Returns an empty dict if the table has no geometry columns.
        """
        cache_key = (target_schema, target_table)
        if cache_key in self._geometry_info_cache:
            return self._geometry_info_cache[cache_key]

        try:
            df = self.query(
                """
                select f_geometry_column, srid
                from geometry_columns
                where f_table_schema = %(schema)s and f_table_name = %(table)s
                """,
                {"schema": target_schema, "table": target_table},
            )
            info = {row["f_geometry_column"]: row["srid"] for _, row in df.iterrows()}
        except Exception:
            info = {}

        self._geometry_info_cache[cache_key] = info
        return info

    def _get_geometry_column(self, target_table: str, target_schema: str) -> str:
        """Look up the geometry column name from PostGIS metadata."""
        info = self._get_geometry_info(target_table, target_schema)
        if not info:
            raise ValueError(
                f"No geometry column found for {target_schema}.{target_table}"
            )
        return next(iter(info))

    def stream_to_destination(
        self,
        sql: str,
        process_batch: callable,
        params: dict[str, Any] | tuple | None = None,
        batch_size: int = 10000,
    ) -> int:
        total = 0
        for batch in self.query_batches(
            sql, params=params, batch_size=batch_size, as_dicts=True
        ):
            process_batch(batch)
            total += len(batch)
        return total

    @staticmethod
    def _normalize_json_values(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        import ast
        import json

        def _to_json(val: Any) -> Any:
            if isinstance(val, dict):
                return json.dumps({k: _to_json(v) for k, v in val.items()})
            if isinstance(val, str) and val.startswith("{"):
                try:
                    parsed = ast.literal_eval(val)
                    if isinstance(parsed, dict):
                        return json.dumps({k: _to_json(v) for k, v in parsed.items()})
                except (ValueError, SyntaxError):
                    pass
            return val

        for row in rows:
            for key, val in row.items():
                row[key] = _to_json(val)
        return rows

    @pg_retry()
    def ingest_batch(
        self,
        rows: list[dict[str, Any]],
        target_table: str,
        target_schema: str,
        conflict_column: str | list[str] | None = None,
        conflict_action: str = "NOTHING",
    ) -> int:
        """
        Bulk insert a list of dicts into *target_table* using COPY
        via a staging table for efficiency.

        Geometry columns work automatically — pass WKT or WKB hex strings.

        Args:
            rows:              List of dicts to insert.
            target_table:      Table name.
            target_schema:     Schema name.
            conflict_column:   Column name (str) or list of column names for
                               composite keys. Used in ON CONFLICT clause.
            conflict_action:   "NOTHING" (skip) or "UPDATE" (upsert).

        Returns:
            Number of rows inserted.
        """
        if not rows:
            return 0
        fqn = f"{target_schema}.{target_table}"
        rows = self._normalize_json_values(rows)
        columns = list(rows[0].keys())
        col_list = ", ".join(f'"{c}"' for c in columns)

        if isinstance(conflict_column, str):
            conflict_columns = [conflict_column]
        else:
            conflict_columns = conflict_column

        with self.cursor() as cur:
            cur.execute(f"""
                create temp table _staging (like {fqn} including defaults)
                on commit drop
            """)

            buf = io.StringIO()
            for row in rows:
                vals = []
                for c in columns:
                    v = row.get(c)
                    if v is None:
                        vals.append("\\N")
                    else:
                        vals.append(
                            str(v)
                            .replace("\\", "\\\\")
                            .replace("\t", " ")
                            .replace("\n", " ")
                        )
                buf.write("\t".join(vals) + "\n")
            buf.seek(0)

            cur.copy_expert(
                f"copy _staging ({col_list}) from stdin with (format text, NULL '\\N')",
                buf,
            )

            insert_sql = (
                f"insert into {fqn} ({col_list}) select {col_list} from _staging"
            )

            if conflict_columns:
                conflict_clause = ", ".join(f'"{c}"' for c in conflict_columns)
                if conflict_action.upper() == "UPDATE":
                    update_cols = [c for c in columns if c not in conflict_columns]
                    set_clause = ", ".join(
                        f'"{c}" = excluded."{c}"' for c in update_cols
                    )
                    insert_sql += (
                        f" on conflict ({conflict_clause}) do update set {set_clause}"
                    )
                else:
                    insert_sql += f" on conflict ({conflict_clause}) do nothing"

            cur.execute(insert_sql)
            count = cur.rowcount

        self.logger.info("Inserted %d rows into %s", count, fqn)
        return count

    @pg_retry()
    def ingest_csv(
        self, filepath: str | Path, target_table: str, target_schema: str
    ) -> int:
        fqn = f"{target_schema}.{target_table}"
        with self.cursor() as cur:
            cur.execute("drop table if exists _staging")
            cur.execute(f"create temp table _staging (like {fqn} excluding all)")
            cur.execute('alter table _staging drop column if exists "ingested_at"')
            with open(filepath, "r") as f:
                cur.copy_expert("copy _staging from stdin with csv header", f)
            cur.execute(f"""
                insert into {fqn}
                select *, current_timestamp at time zone 'UTC'
                from _staging
            """)
            return cur.rowcount

    @pg_retry()
    def ingest_geojson(
        self,
        filepath: str | Path,
        target_table: str,
        target_schema: str,
        conflict_column: str | list[str] | None = None,
        conflict_action: str = "NOTHING",
        batch_size: int = 5000,
        srid: int = 4326,
    ) -> tuple[int, list[dict]]:
        """
        Stream-ingest a GeoJSON file into target_table via a staging table.

        Uses ijson for constant-memory parsing regardless of file size.

        Args:
            filepath:          Path to .geojson file.
            target_table:      Fully-qualified table name (e.g. "public.parcels").
            conflict_column:   Column(s) for ON CONFLICT. None = plain INSERT.
            conflict_action:   "NOTHING" or "UPDATE".
            batch_size:        Rows per COPY batch.
            srid:              Spatial reference ID (default 4326/WGS84).
            geometry_column:   Name of the geometry column in the target table.

        Returns:
            (inserted_count, failed_features) — failed_features contains dicts
            with 'index', 'feature', and 'error' keys.
        """
        import ijson
        from shapely.geometry import shape
        from shapely import wkb as shapely_wkb

        filepath = Path(filepath)
        fqn = f"{target_schema}.{target_table}"
        geometry_column = self._get_geometry_column(target_table, target_schema)
        self.logger.info(f"Detected geometry column '{geometry_column}' in {fqn}")
        failed: list[dict] = []

        if isinstance(conflict_column, str):
            conflict_columns = [conflict_column]
        else:
            conflict_columns = conflict_column

        columns: list[str] | None = None
        col_list: str | None = None

        with self.cursor() as cur:
            cur.execute(f"""
                create temp table _geojson_staging (like {fqn} including defaults)
                on commit drop
            """)

            def _flush_batch(buf: io.StringIO, col_list: str) -> None:
                buf.seek(0)
                cur.copy_expert(
                    f"copy _geojson_staging ({col_list}) from stdin with (format text, NULL '\\N')",
                    buf,
                )

            buf = io.StringIO()
            batch_count = 0
            feature_index = 0

            with open(filepath, "rb") as f:
                for feat in ijson.items(f, "features.item"):
                    try:
                        props = feat.get("properties") or {}
                        geom = feat.get("geometry")
                        if geom is None:
                            raise ValueError("Feature has no geometry")

                        wkb_hex = shapely_wkb.dumps(shape(geom), hex=True, srid=srid)

                        # Discover columns from the first valid feature
                        if columns is None:
                            columns = list(props.keys()) + [geometry_column]
                            col_list = ", ".join(f'"{c}"' for c in columns)

                        prop_columns = columns[:-1]  # all except geometry
                        vals = []
                        for c in prop_columns:
                            v = props.get(c)
                            if v is None:
                                vals.append("\\N")
                            else:
                                vals.append(
                                    str(v)
                                    .replace("\\", "\\\\")
                                    .replace("\t", " ")
                                    .replace("\n", " ")
                                )
                        vals.append(wkb_hex)
                        buf.write("\t".join(vals) + "\n")
                        batch_count += 1

                        if batch_count >= batch_size:
                            _flush_batch(buf, col_list)
                            buf.close()
                            buf = io.StringIO()
                            batch_count = 0

                    except Exception as e:
                        self.logger.debug("Skipping feature %d: %s", feature_index, e)
                        failed.append(
                            {
                                "index": feature_index,
                                "feature": feat,
                                "error": str(e),
                            }
                        )

                    feature_index += 1
                if failed:
                    self.logger.warning(
                        "Skipped %d features in %s (first error: %s)",
                        len(failed),
                        filepath.name,
                        failed[0]["error"],
                    )

            # Flush remaining rows
            if batch_count > 0 and col_list is not None:
                _flush_batch(buf, col_list)
            buf.close()

            if columns is None:
                self.logger.error("No valid features found in %s", filepath)
                return 0, failed

            # Cast geometry text → PostGIS geometry
            cur.execute(f"""
                alter table _geojson_staging
                alter column "{geometry_column}" type geometry
                using "{geometry_column}"::geometry
            """)

            # Merge staging → target
            insert_sql = (
                f"insert into {fqn} ({col_list}) "
                f"select {col_list} from _geojson_staging"
            )

            if conflict_columns:
                conflict_clause = ", ".join(f'"{c}"' for c in conflict_columns)
                if conflict_action.upper() == "UPDATE":
                    update_cols = [c for c in columns if c not in conflict_columns]
                    set_clause = ", ".join(
                        f'"{c}" = excluded."{c}"' for c in update_cols
                    )
                    insert_sql += (
                        f" on conflict ({conflict_clause}) do update set {set_clause}"
                    )
                else:
                    insert_sql += f" on conflict ({conflict_clause}) do nothing"

            cur.execute(insert_sql)
            inserted = cur.rowcount

        self.logger.info(
            "Inserted %d rows into %s (%d failed)", inserted, fqn, len(failed)
        )
        return inserted, failed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class MySQLEngine:
    def __init__(
        self,
        creds: DatabaseCredentials,
        db_name: Optional[str] = None,
    ) -> None:
        self.creds = creds
        self.db_name = db_name or creds.database
        self._conn: Optional[pymysql.Connection] = None
        self.logger = get_logger("mysql_engine")

    def _connect(self) -> pymysql.Connection:
        print(
            f"Connecting with: host={self.creds.host}, port={self.creds.port}, user={self.creds.username}"
        )
        return pymysql.connect(
            host=self.creds.host,
            port=int(self.creds.port),
            user=self.creds.username,
            password=self.creds.password,
            database=self.db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )

    @property
    def conn(self) -> pymysql.Connection:
        if self._conn is None or not self._conn.open:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None and self._conn.open:
            self._conn.close()
            self._conn = None

    @contextmanager
    def cursor(self):
        cursor = self.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @mysql_retry()
    def query(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        with self.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()

    @mysql_retry()
    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        with self.cursor() as cur:
            result = cur.execute(sql, params)
            self.conn.commit()
            return result

    @mysql_retry()
    def upsert_batch(
        self,
        table: str,
        records: list[dict],
        update_columns: Optional[list[str]] = None,
    ) -> int:
        if not records:
            return 0

        columns = list(records[0].keys())
        update_cols = update_columns or columns

        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(f"`{c}`" for c in columns)
        update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)

        sql = f"""
            insert into `{table}` ({columns_str})
            values ({placeholders})
            on duplicate key update {update_clause}
        """

        values = [tuple(r[col] for col in columns) for r in records]

        with self.cursor() as cur:
            result = cur.executemany(sql, values)
            self.conn.commit()
            return result

    def upsert_batches(
        self,
        table: str,
        batches: Iterator[list[dict]],
        update_columns: Optional[list[str]] = None,
    ) -> int:
        total = 0
        for batch in batches:
            total += self.upsert_batch(table, batch, update_columns)
        return total

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
