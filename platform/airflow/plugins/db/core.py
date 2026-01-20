from contextlib import contextmanager
from typing import Any, Generator, Optional
import warnings

import pymysql
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor


class MySQLConnector:
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
        charset: str = "utf8mb4",
        **kwargs,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        self.connection_params = kwargs
        self.connection: Optional[pymysql.connections.Connection] = None
        self.connect()

    def connect(self) -> None:
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor,
                **self.connection_params,
            )
            print(
                f"Successfully connected to database '{self.database}' at {self.host}"
            )
        except pymysql.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

    def disconnect(self) -> None:
        if self.connection and self.connection.open:
            self.connection.close()
            print("Database connection closed")

    def query(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        if not self.connection or not self.connection.open:
            raise ConnectionError("Not connected to database. Call connect() first.")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                if cursor.description is None:
                    warnings.warn(
                        "Query did not return results (likely INSERT/UPDATE/DELETE). "
                        "Use execute() method instead.",
                        UserWarning,
                    )
                    return pd.DataFrame()
                results = cursor.fetchall()
                if results:
                    df = pd.DataFrame(results)
                else:
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(columns=columns)
                return df
        except pymysql.Error as e:
            raise RuntimeError(f"Query execution failed: {e}")

    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        if not self.connection or not self.connection.open:
            raise ConnectionError("Not connected to database. Call connect() first.")

        try:
            with self.connection.cursor() as cursor:
                affected_rows = cursor.execute(sql, params)
                self.connection.commit()
                return affected_rows

        except pymysql.Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Statement execution failed: {e}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False


class PostgresDB:
    def __init__(
        self, host: str, port: int, database: str, user: str, password: str, **kwargs
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            **kwargs,
        }
        self._connection = None

    def connect(self):
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(**self.connection_params)
        return self._connection

    def close(self):
        if self._connection and not self._connection.closed:
            self._connection.close()

    @contextmanager
    def get_cursor(self, cursor_factory=None):
        conn = self.connect()
        cursor = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()

    def query(
        self, sql: str, params: Optional[tuple] = None, return_dict: bool = True
    ) -> list[dict[str, Any]]:
        """ Execute a SELECT query and return all results.
            Args:   sql: SQL query string
                    params: Query parameters (tuple)
                    return_dict: If True, return list of dicts; if False, return list of tuples
            Returns: List of query results """
        cursor_factory = RealDictCursor if return_dict else None
        with self.get_cursor(cursor_factory=cursor_factory) as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
            if return_dict:
                return [dict(row) for row in results]
            return results

    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount

    def execute_many(self, sql: str, params_list: list[tuple]) -> int:
        """ Execute the same statement multiple times with different parameters.
            Args:   sql: SQL statement
                    params_list: List of parameter tuples
            Returns: Number of affected rows """
        with self.get_cursor() as cursor:
            cursor.executemany(sql, params_list)
            return cursor.rowcount

    def stream_batches(
        self,
        sql: str,
        params: Optional[tuple] = None,
        batch_size: int = 1000,
        return_dict: bool = True,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """ Stream query results in batches using a server-side cursor.
            Memory-efficient for large result sets.

            Args:   sql: SQL query string
                    params: Query parameters (tuple)
                    batch_size: Number of rows per batch
                    return_dict: If True, yield dicts; if False, yield tuples
            Yields: Batches of rows (list of dicts or tuples) """
        cursor_factory = RealDictCursor if return_dict else None
        conn = self.connect()
        with conn.cursor(
            name="server_side_cursor", cursor_factory=cursor_factory
        ) as cursor:
            cursor.itersize = batch_size  # Set fetch size
            cursor.execute(sql, params)
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                if return_dict:
                    yield [dict(row) for row in batch]
                else:
                    yield batch

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
