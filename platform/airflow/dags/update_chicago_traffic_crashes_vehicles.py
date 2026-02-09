import datetime as dt
import logging
import os

from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.operator import chain

from db.af_utils import get_postgres_engine
from collectors.socrata import SocrataCollector
from collectors.ingestion_tracker import IngestionTracker

task_logger = logging.getLogger("airflow.task")


def get_connection_dict(conn_id: str) -> dict:
    """Extract connection parameters from Airflow connection."""
    conn = BaseHook.get_connection(conn_id)
    return {
        "host": conn.host,
        "port": conn.port or 5432,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password,
    }


@dag(
    schedule="0 20 20 1 *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["tiger", "geocoder"],
)
def update_chicago_traffic_crashes_vehicles():

    @task
    def run_full_update(conn_id: str, dataset: str) -> bool:
        pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
        df = pg_engine.query(
            """ SELECT * FROM raw_data.chicago_traffic_crashes_vehicles LIMIT 0 """
        )
        task_logger.info(f"Table columns: {df} ")
        search_path = pg_engine.query(""" SHOW search_path """)
        task_logger.info(f"search_path: {search_path} ")
        tracker = IngestionTracker(engine=pg_engine)
        socrata_collector = SocrataCollector(
            engine=pg_engine,
            tracker=tracker,
            app_token=os.environ["SOCRATA_APP_TOKEN"],
        )
        target_table = "chicago_traffic_crashes_vehicles"
        rows = socrata_collector.full_refresh(
            dataset_id=dataset,
            target_table=target_table,
            target_schema="raw_data",
        )
        task_logger.info(f"Ingested {rows} rows into table '{target_table}' ")
        return True

    @task
    def check_ingestion_log(conn_id: str, dataset: str) -> bool:
        pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
        log_record = pg_engine.query(
            """
            select *
            from meta.ingest_log
            where dataset = %(dataset)s
            order by completed_at desc limit 1
            """,
            {"dataset": dataset},
        )
        task_logger.info(f"Ingestion log record: {log_record}")
        return True

    _run_full_update = run_full_update(conn_id="gis_dwh_db", dataset="68nd-jvt3")
    _check_ingestion_log = check_ingestion_log(
        conn_id="gis_dwh_db", dataset="68nd-jvt3"
    )
    chain(_run_full_update, _check_ingestion_log)


update_chicago_traffic_crashes_vehicles()
