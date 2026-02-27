import datetime as dt
import logging

from airflow.sdk import dag
from loci.sources.dataset_specs import LINEARWATER_TIGER_SPEC as DATASET_SPEC
from loci.sources.update_configs import LINEARWATER_TIGER_UPDATE_CONFIG as UPDATE_CONFIG
from loci.tasks.tiger_tasks import update_tiger_table

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["tiger", "gis", "water"],
)
def update_tiger_linear_water():
    update_tiger_table(
        update_config=UPDATE_CONFIG,
        dataset_spec=DATASET_SPEC,
        conn_id=CONN_ID,
        task_logger=task_logger,
    )


update_tiger_linear_water()
