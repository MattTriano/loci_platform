import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.collectors.threedep.taskflow import update_threedep_table
from loci.sources.update_configs import CHICAGO_3DEP_ELEVATION_UC as UPDATE_CONFIG

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["chicago", "3dep", "elevation"],
)
def update_chicago_3dep_elevation():
    update_threedep_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_chicago_3dep_elevation()
