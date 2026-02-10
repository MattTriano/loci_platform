import datetime as dt
import logging

from airflow.sdk import dag

from sources.update_configs import (
    CHICAGO_311_SERVICE_REQUESTS as UPDATE_CONFIG,
)
from tasks.socrata_tasks import update_socrata_table


task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["socrata", "update"],
)
def update_chicago_311_service_requests():
    update_socrata_table(
        update_config=UPDATE_CONFIG, conn_id=CONN_ID, task_logger=task_logger
    )


update_chicago_311_service_requests()
