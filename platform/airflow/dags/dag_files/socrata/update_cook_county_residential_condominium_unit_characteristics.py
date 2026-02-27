import datetime as dt
import logging

from airflow.sdk import dag

from loci.sources.update_configs import (
    COOK_COUNTY_RESIDENTIAL_CONDOMINIUM_UNIT_CHARACTERISTICS as UPDATE_CONFIG,
)
from loci.tasks.socrata_tasks import update_socrata_table


task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["socrata", "update", "Cook County", "parcels", "real estate"],
)
def update_cook_county_residential_condominium_unit_characteristics():
    update_socrata_table(
        update_config=UPDATE_CONFIG, conn_id=CONN_ID, task_logger=task_logger
    )


update_cook_county_residential_condominium_unit_characteristics()
