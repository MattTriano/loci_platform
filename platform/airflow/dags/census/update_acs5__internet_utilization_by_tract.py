import datetime as dt
from logging import getLogger

from airflow.sdk import dag

from sources.dataset_specs import ACS5__INTERNET_UTILIZATION_BY_TRACT_SPEC as DATASET_SPEC
from sources.update_configs import (
    ACS5__INTERNET_UTILIZATION_BY_TRACT_UPDATE_CONFIG as UPDATE_CONFIG,
)
from tasks.census_tasks import update_census_table

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["census", "update", "housing", "tract"],
)
def update_acs5__internet_utilization_by_tract():
    update_census_table(
        conn_id=CONN_ID,
        dataset_spec=DATASET_SPEC,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_acs5__internet_utilization_by_tract()
