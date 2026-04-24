import datetime as dt
import logging

from airflow.sdk import Param, dag
from loci.sources.update_configs import (
    COOK_COUNTY_ASSESSED_PARCEL_VALUES_UC as UPDATE_CONFIG,
)
from loci.tasks.socrata_tasks import update_socrata_table

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["socrata", "update", "Cook County", "parcels", "real estate"],
    params={
        "force_full_refresh": Param(
            False,
            type="boolean",
            title="Force full refresh",
            description=(
                "If enabled, routes all datasets in this DAG run through run_full_update "
                "regardless of the scheduled full-refresh cadence. Useful for manually "
                "triggering a full refresh on a giant dataset without waiting for the "
                "next scheduled full-update day."
            ),
        ),
    },
)
def update_cook_county_assessed_parcel_values():
    update_socrata_table(update_config=UPDATE_CONFIG, conn_id=CONN_ID, task_logger=task_logger)


update_cook_county_assessed_parcel_values()
