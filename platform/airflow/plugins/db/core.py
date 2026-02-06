import logging
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus
from logging import Logger
from typing import Any, Iterator, Optional

import pymysql
import pandas as pd
import psycopg2
import psycopg2.extras
from airflow.sdk.bases.hook import BaseHook
from psycopg2.extensions import connection as Psycopg2Connection
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


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
    def from_env_file(cls, env_path: str | Path, prefix: str, driver: str) -> "DatabaseCredentials":
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
        retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError)),
        reraise=True,
    )


def mysql_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((pymysql.OperationalError, pymysql.InterfaceError)),
        reraise=True,
    )


class PostgresEngine:
    def __init__(self, creds: DatabaseCredentials, db_name: Optional[str] = None) -> None:
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
    def query(self, sql: str) -> pd.DataFrame:
        with self.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)


    @pg_retry()
    def execute(self, sql: str) -> None:
        try:
            with self.cursor() as cur:
                cur.execute(sql)
        except Exception as e:
            self.logger.error(f"Command failed with error {e}")
            raise

    @pg_retry()
    def query_batches(
        self,
        sql: str,
        batch_size: int = 10000,
        as_dicts: bool = True,
    ) -> Iterator[list[dict] | pd.DataFrame]:
        cursor = self.connection.cursor(name="batch_cursor")
        cursor.itersize = batch_size
        try:
            cursor.execute(sql)
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
        batch_size: int = 10000,
    ) -> int:
        total = 0
        for batch in self.query_batches(sql, batch_size=batch_size, as_dicts=True):
            process_batch(batch)
            total += len(batch)
        return total

    @pg_retry()
    def ingest_batch(
        self,
        rows: list[dict[str, Any]],
        target_table: str,
        conflict_column: str | None = None,
        conflict_action: str = "NOTHING",
    ) -> int:
        """
        Bulk insert a list of dicts into *target_table*.

        Column names are derived from the dict keys of the first row.
        Uses execute_batch for efficient multi-row inserts.

        Args:
            rows:              List of dicts to insert.
            target_table:      Fully-qualified table name (e.g. "raw.crimes").
            conflict_column:   Optional column for ON CONFLICT clause (e.g. "id").
            conflict_action:   What to do on conflict: "NOTHING" (skip) or "UPDATE"
                               (upsert all non-conflict columns). Default: "NOTHING".

        Returns:
            Number of rows submitted for insert.
        """
        if not rows:
            return 0

        columns = list(rows[0].keys())
        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))

        sql = f'INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})'

        if conflict_column:
            if conflict_action.upper() == "UPDATE":
                update_cols = [c for c in columns if c != conflict_column]
                set_clause = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"' for c in update_cols
                )
                sql += f' ON CONFLICT ("{conflict_column}") DO UPDATE SET {set_clause}'
            else:
                sql += f' ON CONFLICT ("{conflict_column}") DO NOTHING'

        tuples = [tuple(row.get(c) for c in columns) for row in rows]

        with self.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, tuples, page_size=1000)

        self.logger.info("Inserted %d rows into %s", len(tuples), target_table)
        return len(tuples)


    @pg_retry()
    def ingest_csv(self, filepath: str | Path, table_name: str, schema: str) -> int:
        fqn = f"{schema}.{table_name}"
        with self.cursor() as cur:
            cur.execute("drop table if exists _staging")
            cur.execute(f"create temp table _staging (like {fqn} excluding all)")
            cur.execute("alter table _staging drop column if exists ingested_to_postgres")
            with open(filepath, "r") as f:
                cur.copy_expert("copy _staging from stdin with csv header", f)
            cur.execute(f"""
                insert into {fqn}
                select *, current_timestamp at time zone 'UTC'
                from _staging
            """)
            return cur.rowcount

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
        print(f"Connecting with: host={self.creds.host}, port={self.creds.port}, user={self.creds.username}")
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


def extract_connection_to_dict(
    conn_id: str, logger: Optional[Logger] = None
) -> dict[str, Any]:
    conn = BaseHook.get_connection(conn_id)
    conn_dict = {
        "type": conn.conn_type,
        "host": conn.host,
        "port": conn.port,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password,
        "extra": conn.extra_dejson,
    }
    if logger is not None:
        safe_dict = conn_dict.copy()
        safe_dict["password"] = "****" if conn_dict["password"] else None
        logger.info(f"Connection parameters: {safe_dict}")
    return conn_dict


def get_postgres_engine(
    conn_id: str, logger: Optional[Logger] = None
) -> PostgresEngine:
    conn_dict = extract_connection_to_dict(conn_id, logger)
    return PostgresEngine(
        DatabaseCredentials(
            host=conn_dict["host"],
            port=conn_dict.get("port", "5432"),
            database=conn_dict["database"],
            username=conn_dict["user"],
            password=conn_dict["password"],
        )
    )


def get_mysql_engine(
    conn_id: str, logger: Optional[Logger] = None
) -> MySQLEngine:
    conn_dict = extract_connection_to_dict(conn_id, logger)
    port = conn_dict.get("port")
    if port is None:
        port = "3306"
    return MySQLEngine(
        DatabaseCredentials(
            host=conn_dict["host"],
            port=port,
            database=conn_dict["database"],
            username=conn_dict["user"],
            password=conn_dict["password"],
        )
    )
