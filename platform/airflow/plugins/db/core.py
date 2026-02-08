import io
import logging
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus
from typing import Any, Iterator, Optional

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

    @pg_retry()
    def query(
        self,
        sql: str,
        params: dict[str, Any] | tuple | None = None,
    ) -> pd.DataFrame:
        """
        Execute a SELECT and return results as a DataFrame.

        Args:
            sql:    SQL string. Use %(name)s for named params or %s for positional.
            params: Dict for named params, tuple for positional, or None.
        """
        with self.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)

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

    @pg_retry()
    def ingest_batch(
        self,
        rows: list[dict[str, Any]],
        target_table: str,
        conflict_column: str | list[str] | None = None,
        conflict_action: str = "NOTHING",
    ) -> int:
        """
        Bulk insert a list of dicts into *target_table* using COPY
        via a staging table for efficiency.

        Geometry columns work automatically — pass WKT or WKB hex strings.

        Args:
            rows:              List of dicts to insert.
            target_table:      Fully-qualified table name (e.g. "raw.crimes").
            conflict_column:   Column name (str) or list of column names for
                               composite keys. Used in ON CONFLICT clause.
            conflict_action:   "NOTHING" (skip) or "UPDATE" (upsert).

        Returns:
            Number of rows inserted.
        """
        if not rows:
            return 0

        columns = list(rows[0].keys())
        col_list = ", ".join(f'"{c}"' for c in columns)

        # Normalize conflict_column to a list
        if isinstance(conflict_column, str):
            conflict_columns = [conflict_column]
        else:
            conflict_columns = conflict_column

        with self.cursor() as cur:
            cur.execute(f"""
                CREATE TEMP TABLE _staging (LIKE {target_table} INCLUDING DEFAULTS)
                ON COMMIT DROP
            """)

            # Stream rows via COPY
            buf = io.StringIO()
            for row in rows:
                vals = []
                for c in columns:
                    v = row.get(c)
                    if v is None:
                        vals.append("\\N")
                    else:
                        vals.append(str(v).replace("\t", " ").replace("\n", " "))
                buf.write("\t".join(vals) + "\n")
            buf.seek(0)

            cur.copy_expert(
                f"COPY _staging ({col_list}) FROM STDIN WITH (FORMAT text, NULL '\\N')",
                buf,
            )

            # Insert from staging into target
            insert_sql = f"INSERT INTO {target_table} ({col_list}) SELECT {col_list} FROM _staging"

            if conflict_columns:
                conflict_clause = ", ".join(f'"{c}"' for c in conflict_columns)
                if conflict_action.upper() == "UPDATE":
                    update_cols = [c for c in columns if c not in conflict_columns]
                    set_clause = ", ".join(
                        f'"{c}" = EXCLUDED."{c}"' for c in update_cols
                    )
                    insert_sql += (
                        f" ON CONFLICT ({conflict_clause}) DO UPDATE SET {set_clause}"
                    )
                else:
                    insert_sql += f" ON CONFLICT ({conflict_clause}) DO NOTHING"

            cur.execute(insert_sql)
            count = cur.rowcount

        self.logger.info("Inserted %d rows into %s", count, target_table)
        return count

    @pg_retry()
    def ingest_csv(self, filepath: str | Path, table_name: str, schema: str) -> int:
        fqn = f"{schema}.{table_name}"
        with self.cursor() as cur:
            cur.execute("drop table if exists _staging")
            cur.execute(f"create temp table _staging (like {fqn} excluding all)")
            cur.execute(
                "alter table _staging drop column if exists ingested_to_postgres"
            )
            with open(filepath, "r") as f:
                cur.copy_expert("copy _staging from stdin with csv header", f)
            cur.execute(f"""
                insert into {fqn}
                select *, current_timestamp at time zone 'UTC'
                from _staging
            """)
            return cur.rowcount

    def _get_geometry_column(self, target_table: str) -> str:
        """Look up the geometry column name from PostGIS metadata."""
        schema, tbl = (
            target_table.split(".") if "." in target_table else ("public", target_table)
        )
        df = self.query(
            """
            SELECT f_geometry_column
            FROM geometry_columns
            WHERE f_table_schema = %(schema)s AND f_table_name = %(table)s
            LIMIT 1
            """,
            {"schema": schema, "table": tbl},
        )
        if df.empty:
            raise ValueError(f"No geometry column found for {target_table}")
        return df.iloc[0, 0]

    @pg_retry()
    def ingest_geojson(
        self,
        filepath: str | Path,
        target_table: str,
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
        geometry_column = self._get_geometry_column(target_table)
        self.logger.info(
            f"Detected geometry column '{geometry_column}' in {target_table}"
        )
        failed: list[dict] = []

        if isinstance(conflict_column, str):
            conflict_columns = [conflict_column]
        else:
            conflict_columns = conflict_column

        columns: list[str] | None = None
        col_list: str | None = None

        with self.cursor() as cur:
            cur.execute(f"""
                CREATE TEMP TABLE _geojson_staging (LIKE {target_table} INCLUDING DEFAULTS)
                ON COMMIT DROP
            """)

            def _flush_batch(buf: io.StringIO, col_list: str) -> None:
                buf.seek(0)
                cur.copy_expert(
                    f"COPY _geojson_staging ({col_list}) FROM STDIN WITH (FORMAT text, NULL '\\N')",
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
                ALTER TABLE _geojson_staging
                ALTER COLUMN "{geometry_column}" TYPE geometry
                USING "{geometry_column}"::geometry
            """)

            # Merge staging → target
            insert_sql = (
                f"INSERT INTO {target_table} ({col_list}) "
                f"SELECT {col_list} FROM _geojson_staging"
            )

            if conflict_columns:
                conflict_clause = ", ".join(f'"{c}"' for c in conflict_columns)
                if conflict_action.upper() == "UPDATE":
                    update_cols = [c for c in columns if c not in conflict_columns]
                    set_clause = ", ".join(
                        f'"{c}" = EXCLUDED."{c}"' for c in update_cols
                    )
                    insert_sql += (
                        f" ON CONFLICT ({conflict_clause}) DO UPDATE SET {set_clause}"
                    )
                else:
                    insert_sql += f" ON CONFLICT ({conflict_clause}) DO NOTHING"

            cur.execute(insert_sql)
            inserted = cur.rowcount

        self.logger.info(
            "Inserted %d rows into %s (%d failed)", inserted, target_table, len(failed)
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
