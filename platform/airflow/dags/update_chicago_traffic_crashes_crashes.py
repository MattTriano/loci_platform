import datetime as dt
import logging
import os

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.sdk.bases.operator import chain
from airflow.task.trigger_rule import TriggerRule
from croniter import croniter

from db.af_utils import get_postgres_engine
from collectors.socrata import SocrataCollector, IncrementalConfig
from collectors.ingestion_tracker import IngestionTracker

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"
DATASET_ID = "85ca-t3if"
DATASET_NAME = "chicago_traffic_crashes_crashes"
DATASET_FULL_UPDATE_CRON = "10 4 1-7 * 0"


@dag(
    schedule="0 20 20 1 *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["socrata", "update"],
)
def update_chicago_traffic_crashes_crashes():

    @task.branch()
    def choose_update_mode(full_update_cron: str) -> str:
        context = get_current_context()
        logical_date = context["logical_date"]
        cron = croniter(full_update_cron, logical_date.subtract(seconds=1))
        next_hit = pendulum.instance(cron.get_next(pendulum.DateTime))
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
        rows = socrata_collector.full_refresh_via_api(
            dataset_id=dataset_id,
            target_table=dataset_name,
            target_schema="raw_data",
        )
        task_logger.info(f"Ingested {rows} rows into table raw_data.{dataset_name} ")
        return True

    @task
    def run_incremental_update(
        conn_id: str, dataset_id: str, dataset_name: str
    ) -> bool:
        pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
        tracker = IngestionTracker(engine=pg_engine)
        socrata_collector = SocrataCollector(
            engine=pg_engine,
            tracker=tracker,
            app_token=os.environ["SOCRATA_APP_TOKEN"],
        )
        rows = socrata_collector.incremental_update(
            dataset_id=dataset_id,
            target_table=dataset_name,
            target_schema="raw_data",
            config=IncrementalConfig(
                incremental_column=":updated_at",
                conflict_key=None,
            ),
        )
        task_logger.info(f"Ingested {rows} rows into table raw_data.{dataset_name} ")
        return True

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def check_ingestion_log(conn_id: str, dataset_id: str) -> bool:
        pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
        log_record = pg_engine.query(
            """
            select *
            from meta.ingest_log
            where dataset_id = %(dataset_id)s
            order by completed_at desc limit 1
            """,
            {"dataset_id": dataset_id},
        )
        task_logger.info(f"Ingestion log record: {log_record}")
        return True

    _update_mode = choose_update_mode(full_update_cron=DATASET_FULL_UPDATE_CRON)
    _full_update = run_full_update(
        conn_id=CONN_ID, dataset_id=DATASET_ID, dataset_name=DATASET_NAME
    )
    _incremental_update = run_incremental_update(
        conn_id=CONN_ID, dataset_id=DATASET_ID, dataset_name=DATASET_NAME
    )
    _check_ingestion_log = check_ingestion_log(conn_id=CONN_ID, dataset_id=DATASET_ID)

    chain(_update_mode, _full_update, _check_ingestion_log)
    chain(_update_mode, _incremental_update, _check_ingestion_log)


update_chicago_traffic_crashes_crashes()
