import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.sources.update_configs import (
    BOSTON_BIKEINDEX_BIKE_THEFTS_UC as UPDATE_CONFIG,
)
from loci.tasks.bike_index_tasks import update_bike_index_table

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["bikeindex", "boston", "theft", "biking"],
)
def update_boston_bikeindex_bike_thefts():
    update_bike_index_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_boston_bikeindex_bike_thefts()
