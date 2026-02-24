from logging import Logger
from typing import Any, Optional

from airflow.sdk.bases.hook import BaseHook

from loci.db.core import MySQLEngine, PostgresEngine, DatabaseCredentials


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


def get_mysql_engine(conn_id: str, logger: Optional[Logger] = None) -> MySQLEngine:
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
