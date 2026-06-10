import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.collectors.dkan.taskflow import update_dkan_table
from loci.sources.update_configs import OPENPAYMENTS_GENERAL_PAYMENTS_UC as UPDATE_CONFIG

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dkan", "healthcare"],
)
def update_openpayments_general_payments():
    update_dkan_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_openpayments_general_payments()
