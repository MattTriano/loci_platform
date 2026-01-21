from abc import ABC, abstractmethod
from contextlib import contextmanager
from logging import Logger
from typing import Any, Generator, Optional, Union
import warnings

import pymysql
import pandas as pd
import psycopg2
from airflow.sdk.bases.hook import BaseHook
from psycopg2.extras import RealDictCursor, execute_batch


class BaseConnector(ABC):
    """Abstract base class defining the standard database connector interface."""

    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the database."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the connection is active."""
        pass

    @abstractmethod
    @contextmanager
    def get_cursor(self, cursor_factory: Optional[Any] = None) -> Generator:
        """Get a cursor with automatic commit/rollback handling."""
        pass

    @abstractmethod
    def query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        return_dict: bool = True,
    ) -> list[dict[str, Any]]:
        """Execute a SELECT query and return results."""
        pass

    @abstractmethod
    def query_df(
        self,
        sql: str,
        params: Optional[tuple] = None,
    ) -> pd.DataFrame:
        """Execute a SELECT query and return results as a DataFrame."""
        pass

    @abstractmethod
    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        """Execute a single statement (INSERT/UPDATE/DELETE)."""
        pass

    @abstractmethod
    def execute_many(self, sql: str, params_list: list[tuple]) -> int:
        """Execute a statement multiple times with different parameters."""
        pass

    @abstractmethod
    def stream_batches(
        self,
        sql: str,
        params: Optional[tuple] = None,
        batch_size: int = 1000,
        return_dict: bool = True,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Stream query results in batches for memory efficiency."""
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class MySQLConnector(BaseConnector):
    """
    MySQL database connector with standardized API.

    Args:
        host: Database host address
        user: Database username
        password: Database password
        database: Database name
        port: Database port (default: 3306)
        charset: Character set (default: utf8mb4)
        autoconnect: Whether to connect immediately (default: True)
        **kwargs: Additional connection parameters passed to pymysql
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
        charset: str = "utf8mb4",
        autoconnect: bool = True,
        **kwargs,
    ):
        self._connection_params = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port,
            "charset": charset,
            "cursorclass": pymysql.cursors.DictCursor,
            **kwargs,
        }
        self._connection: Optional[pymysql.connections.Connection] = None

        if autoconnect:
            self.connect()

    def connect(self) -> None:
        if self._connection is not None and self._connection.open:
            return
        try:
            self._connection = pymysql.connect(**self._connection_params)
        except pymysql.Error as e:
            raise ConnectionError(f"Failed to connect to MySQL database: {e}")

    def close(self) -> None:
        if self._connection is not None and self._connection.open:
            self._connection.close()
            self._connection = None

    def is_connected(self) -> bool:
        return self._connection is not None and self._connection.open

    def _ensure_connected(self) -> None:
        if not self.is_connected():
            raise ConnectionError("Not connected to database. Call connect() first.")

    @contextmanager
    def get_cursor(self, cursor_factory: Optional[Any] = None) -> Generator:
        """
        Get a cursor with automatic commit/rollback handling.

        Args:
            cursor_factory: Cursor class to use (default: DictCursor).
                           Pass pymysql.cursors.Cursor for tuple results.
        """
        self._ensure_connected()

        # Temporarily change cursor class if specified
        original_cursorclass = self._connection_params.get("cursorclass")
        if cursor_factory is not None:
            self._connection.cursorclass = cursor_factory

        cursor = self._connection.cursor()
        try:
            yield cursor
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise e
        finally:
            cursor.close()
            if cursor_factory is not None:
                self._connection.cursorclass = original_cursorclass

    def query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        return_dict: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Execute a SELECT query and return all results.

        Args:
            sql: SQL query string
            params: Query parameters (tuple)
            return_dict: If True, return list of dicts; if False, return list of tuples

        Returns:
            List of query results
        """
        cursor_factory = (
            pymysql.cursors.DictCursor if return_dict else pymysql.cursors.Cursor
        )

        with self.get_cursor(cursor_factory=cursor_factory) as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
            if return_dict:
                return [dict(row) for row in results]
            return list(results)

    def query_df(
        self,
        sql: str,
        params: Optional[tuple] = None,
    ) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as a DataFrame.

        Args:
            sql: SQL query string
            params: Query parameters (tuple)

        Returns:
            pandas DataFrame with query results
        """
        with self.get_cursor() as cursor:
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
                return pd.DataFrame(results)
            else:
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(columns=columns)

    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        """
        Execute a single statement (INSERT/UPDATE/DELETE).

        Args:
            sql: SQL statement
            params: Statement parameters (tuple)

        Returns:
            Number of affected rows
        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount

    def execute_many(self, sql: str, params_list: list[tuple]) -> int:
        """
        Execute the same statement multiple times with different parameters.

        Uses executemany for batch operations.

        Args:
            sql: SQL statement
            params_list: List of parameter tuples

        Returns:
            Number of affected rows
        """
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
        """
        Stream query results in batches for memory efficiency.

        Note: MySQL doesn't have true server-side cursors like PostgreSQL,
        so this uses SSCursor (unbuffered cursor) for streaming.

        Args:
            sql: SQL query string
            params: Query parameters (tuple)
            batch_size: Number of rows per batch
            return_dict: If True, yield dicts; if False, yield tuples

        Yields:
            Batches of rows (list of dicts or tuples)
        """
        self._ensure_connected()

        cursor_class = (
            pymysql.cursors.SSDictCursor if return_dict else pymysql.cursors.SSCursor
        )
        cursor = self._connection.cursor(cursor_class)

        try:
            cursor.execute(sql, params)
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                if return_dict:
                    yield [dict(row) for row in batch]
                else:
                    yield list(batch)
        finally:
            cursor.close()


class PostgresConnector(BaseConnector):
    """
    PostgreSQL database connector with standardized API.

    Args:
        host: Database host address
        user: Database username
        password: Database password
        database: Database name
        port: Database port (default: 5432)
        autoconnect: Whether to connect immediately (default: True)
        **kwargs: Additional connection parameters passed to psycopg2
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 5432,
        autoconnect: bool = True,
        **kwargs,
    ):
        self._connection_params = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port,
            **kwargs,
        }
        self._connection = None

        if autoconnect:
            self.connect()

    def connect(self) -> None:
        if self._connection is not None and not self._connection.closed:
            return
        try:
            self._connection = psycopg2.connect(**self._connection_params)
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL database: {e}")

    def close(self) -> None:
        if self._connection is not None and not self._connection.closed:
            self._connection.close()
            self._connection = None

    def is_connected(self) -> bool:
        return self._connection is not None and not self._connection.closed

    def _ensure_connected(self) -> None:
        if not self.is_connected():
            raise ConnectionError("Not connected to database. Call connect() first.")

    @contextmanager
    def get_cursor(self, cursor_factory: Optional[Any] = None) -> Generator:
        """
        Get a cursor with automatic commit/rollback handling.

        Args:
            cursor_factory: Cursor factory to use (e.g., RealDictCursor)
        """
        self._ensure_connected()
        cursor = self._connection.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise e
        finally:
            cursor.close()

    def query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        return_dict: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Execute a SELECT query and return all results.

        Args:
            sql: SQL query string
            params: Query parameters (tuple)
            return_dict: If True, return list of dicts; if False, return list of tuples

        Returns:
            List of query results
        """
        cursor_factory = RealDictCursor if return_dict else None

        with self.get_cursor(cursor_factory=cursor_factory) as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
            if return_dict:
                return [dict(row) for row in results]
            return results

    def query_df(
        self,
        sql: str,
        params: Optional[tuple] = None,
    ) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as a DataFrame.

        Args:
            sql: SQL query string
            params: Query parameters (tuple)

        Returns:
            pandas DataFrame with query results
        """
        with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
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
                return pd.DataFrame([dict(row) for row in results])
            else:
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(columns=columns)

    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        """
        Execute a single statement (INSERT/UPDATE/DELETE).

        Args:
            sql: SQL statement
            params: Statement parameters (tuple)

        Returns:
            Number of affected rows
        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount

    def execute_many(
        self, sql: str, params_list: list[tuple], page_size: int = 1000
    ) -> int:
        """
        Execute the same statement multiple times with different parameters.

        Uses psycopg2's execute_batch for significantly better performance
        than executemany (batches statements together).

        Args:
            sql: SQL statement
            params_list: List of parameter tuples
            page_size: Number of statements per batch (default: 1000)

        Returns:
            Number of affected rows
        """
        with self.get_cursor() as cursor:
            execute_batch(cursor, sql, params_list, page_size=page_size)
            return cursor.rowcount

    def stream_batches(
        self,
        sql: str,
        params: Optional[tuple] = None,
        batch_size: int = 1000,
        return_dict: bool = True,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """
        Stream query results in batches using a server-side cursor.

        Memory-efficient for large result sets. Uses PostgreSQL's
        server-side cursor to avoid loading all results into memory.

        Args:
            sql: SQL query string
            params: Query parameters (tuple)
            batch_size: Number of rows per batch
            return_dict: If True, yield dicts; if False, yield tuples

        Yields:
            Batches of rows (list of dicts or tuples)
        """
        self._ensure_connected()

        cursor_factory = RealDictCursor if return_dict else None

        # Named cursor for server-side processing
        with self._connection.cursor(
            name="server_side_cursor", cursor_factory=cursor_factory
        ) as cursor:
            cursor.itersize = batch_size
            cursor.execute(sql, params)

            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                if return_dict:
                    yield [dict(row) for row in batch]
                else:
                    yield batch


DatabaseConnector = Union[MySQLConnector, PostgresConnector]


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


def get_postgres_connector(
    conn_id: str, logger: Optional[Logger] = None
) -> PostgresConnector:
    conn_dict = extract_connection_to_dict(conn_id, logger)
    return PostgresConnector(
        host=conn_dict["host"],
        port=conn_dict["port"],
        database=conn_dict["database"],
        user=conn_dict["user"],
        password=conn_dict["password"],
    )


def get_mysql_connector(
    conn_id: str, logger: Optional[Logger] = None
) -> MySQLConnector:
    conn_dict = extract_connection_to_dict(conn_id, logger)
    return MySQLConnector(
        host=conn_dict["host"],
        port=conn_dict["port"],
        database=conn_dict["database"],
        user=conn_dict["user"],
        password=conn_dict["password"],
    )
