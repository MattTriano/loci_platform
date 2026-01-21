import datetime as dt
import logging

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain

from db.core import get_postgres_connector, get_mysql_connector

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule="0 20 20 1 *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["mock_data"],
)
def development_dag():
    @task
    def show_postgres_schemas() -> list[dict[str, str]]:
        pg_connector = get_postgres_connector(conn_id="dwh_db", logger=task_logger)
        schemas = pg_connector.query(""" select * from information_schema.schemata """)
        for schema in schemas:
            print(schema)
        # task_logger.log(schemas)
        return schemas

    @task
    def show_mysql_schemas() -> list[dict[str, str]]:
        pg_connector = get_mysql_connector(conn_id="app_db", logger=task_logger)
        schemas = pg_connector.query(""" show schemas """)
        for schema in schemas:
            print(schema)
        # task_logger.log(schemas)
        return schemas

    pg_schemas = show_postgres_schemas()
    mysql_schemas = show_mysql_schemas()
    chain([pg_schemas, mysql_schemas])


development_dag()
