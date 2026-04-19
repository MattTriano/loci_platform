import datetime as dt
import logging

from airflow.sdk import dag
from loci.sources.update_configs import DETROIT_BIKE_PARKING_UC as UPDATE_CONFIG
from loci.tasks.arcgishub_tasks import update_arcgishub_table

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["arcgishub", "biking", "detroit"],
)
def update_detroit_bike_parking():
    update_arcgishub_table(update_config=UPDATE_CONFIG, conn_id=CONN_ID, task_logger=task_logger)


update_detroit_bike_parking()
