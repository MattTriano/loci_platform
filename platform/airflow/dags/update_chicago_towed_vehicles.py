import datetime as dt
import logging
import os

from airflow.sdk import dag, task, get_current_context
# from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.operator import chain
from croniter import croniter

from db.af_utils import get_postgres_engine
from collectors.socrata import SocrataCollector
from collectors.ingestion_tracker import IngestionTracker

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"
DATASET_ID = "ygr5-vcbg"
DATASET_NAME = "chicago_towed_vehicles"
DATASET_FULL_UPDATE_CRON = "0 4 1-7 * 0"


# def get_connection_dict(conn_id: str) -> dict:
#     """Extract connection parameters from Airflow connection."""
#     conn = BaseHook.get_connection(conn_id)
#     return {
#         "host": conn.host,
#         "port": conn.port or 5432,
#         "database": conn.schema,
#         "user": conn.login,
#         "password": conn.password,
#     }


@dag(
    schedule="0 20 20 1 *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["tiger", "geocoder"],
)
def update_chicago_towed_vehicles():

    @task.branch
    def choose_update_mode(full_update_cron: str, force_full_update: bool = False) -> str:
        context = get_current_context()
        logical_date = context["logical_date"]
        cron = croniter(full_update_cron, logical_date.subtract(seconds=1))
        next_hit = cron.get_next(DateTime)
        # If the next cron hit matches the logical_date, it's a full-update run
        if next_hit.date() == logical_date.date():
            return "run_full_update"
        return "run_incremental_update"

    @task
    def run_full_update(conn_id: str, dataset_id: str, dataset_name: str) -> bool:
        pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
        tracker = IngestionTracker(engine=pg_engine)
        socrata_collector = SocrataCollector(
            engine=pg_engine,
            tracker=tracker,
            app_token=os.environ["SOCRATA_APP_TOKEN"],
        )
        rows = socrata_collector.full_refresh(
            dataset_id=dataset_id,
            target_table=dataset_name,
            target_schema="raw_data",
        )
        task_logger.info(f"Ingested {rows} rows into table raw_data.{dataset_name} ")
        return True

    @task
    def check_ingestion_log(conn_id: str, dataset_id: str) -> bool:
        pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
        log_record = pg_engine.query(
            """
            select *
            from meta.ingest_log
            where dataset = %(dataset_id)s
            order by completed_at desc limit 1
            """,
            {"dataset_id": dataset_id},
        )
        task_logger.info(f"Ingestion log record: {log_record}")
        return True

    _run_full_update = run_full_update(
        conn_id=CONN_ID, dataset_id=DATASET_ID, dataset_name=DATASET_NAME
    )
    _check_ingestion_log = check_ingestion_log(conn_id=CONN_ID, dataset_id=DATASET_ID)
    chain(_run_full_update, _check_ingestion_log)


update_chicago_towed_vehicles()
