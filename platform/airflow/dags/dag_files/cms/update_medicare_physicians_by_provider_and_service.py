import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.collectors.cms.taskflow import update_cms_table
from loci.sources.update_configs import (
    MEDICARE_PHYSICIANS_BY_PROVIDER_AND_SERVICE_UC as UPDATE_CONFIG,
)

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cms", "healthcare"],
)
def update_medicare_physicians_by_provider_and_service():
    update_cms_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_medicare_physicians_by_provider_and_service()
