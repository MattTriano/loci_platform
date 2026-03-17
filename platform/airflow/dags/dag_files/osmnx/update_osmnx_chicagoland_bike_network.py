import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.sources.update_configs import OSMNX_CHICAGO_BIKE_NETWORK_UC as UPDATE_CONFIG
from loci.tasks.osmnx_tasks import update_osmnx_table

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["osmnx", "biking", "ways", "nodes"],
)
def update_osmnx_chicagoland_bike_network():
    update_osmnx_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_osmnx_chicagoland_bike_network()
