import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.collectors.static.taskflow import update_static_file_table
from loci.sources.update_configs import AHRQ_HEALTH_SYSTEMS_UC as UPDATE_CONFIG

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["healthcare"],
)
def update_ahrq_chsp_health_systems():
    update_static_file_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_ahrq_chsp_health_systems()
