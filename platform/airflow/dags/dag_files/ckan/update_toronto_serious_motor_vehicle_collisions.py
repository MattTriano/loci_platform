import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.collectors.ckan.taskflow import update_ckan_table
from loci.sources.update_configs import TORONTO_SERIOUS_MOTOR_VEHICLE_COLLISIONS_UC as UPDATE_CONFIG

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["ckan", "traffic", "crashes", "toronto"],
)
def update_toronto_serious_motor_vehicle_collisions():
    update_ckan_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_toronto_serious_motor_vehicle_collisions()
