import datetime as dt
from logging import getLogger

from airflow.sdk import dag
from loci.sources.update_configs import DENVER_OSM_BIKE_NETWORK_EDGES_UC as UPDATE_CONFIG
from loci.tasks.osm_tasks import update_osm_table

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["osm", "denver", "biking", "network"],
)
def update_denver_osm_bike_newtork_edges():
    update_osm_table(
        conn_id=CONN_ID,
        update_config=UPDATE_CONFIG,
        task_logger=task_logger,
    )


update_denver_osm_bike_newtork_edges()
