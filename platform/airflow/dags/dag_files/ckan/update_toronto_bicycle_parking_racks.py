import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.sources.update_configs import TORONTO_BICYCLE_PARKING_RACKS_UC as UPDATE_CONFIG
from loci.tasks.ckan_tasks import update_ckan_table

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["ckan", "traffic", "crashes", "toronto"],
)
def update_toronto_bicycle_parking_racks():
    update_ckan_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_toronto_bicycle_parking_racks()
