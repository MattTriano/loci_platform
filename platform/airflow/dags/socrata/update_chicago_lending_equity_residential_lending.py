import datetime as dt
import logging

from airflow.sdk import dag

from loci.sources.update_configs import (
    CHICAGO_LENDING_EQUITY_RESIDENTIAL_LENDING as UPDATE_CONFIG,
)
from loci.tasks.socrata_tasks import update_socrata_table


task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["socrata", "update", "chicago", "finance", "real estate"],
)
def update_chicago_lending_equity_residential_lending():
    update_socrata_table(
        update_config=UPDATE_CONFIG, conn_id=CONN_ID, task_logger=task_logger
    )


update_chicago_lending_equity_residential_lending()
