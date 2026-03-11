import logging
from datetime import datetime

from airflow.sdk import dag
from loci.tasks.transform_tasks import dbt_build_with_geocoding

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt"],
)
def dbt_build_chicago_data_with_geocoding_step():
    dbt_build_with_geocoding(
        conn_id=CONN_ID,
        task_logger=task_logger,
        restrict_region="ST_MakeEnvelope(-87.94, 41.64, -87.52, 42.03, 4269)",
    )


dbt_build_chicago_data_with_geocoding_step()
