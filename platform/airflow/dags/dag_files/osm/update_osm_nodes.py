import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.sources.dataset_specs import OSM_NODES_SPEC as DATASET_SPEC
from loci.sources.update_configs import OSM_NODES_UPDATE_CONFIG as UPDATE_CONFIG
from loci.tasks.osm_tasks import update_osm_table

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["osm", "nodes"],
)
def update_osm_nodes():
    update_osm_table(
        conn_id=CONN_ID,
        dataset_spec=DATASET_SPEC,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_osm_nodes()
