import logging
from datetime import datetime

from airflow.sdk import dag, task, task_group
from airflow.sdk.bases.operator import chain
from loci.tasks.deploy_tasks import deploy_bike_map
from loci.tasks.export_tasks import export_bike_map_geojson
from loci.tasks.transform_tasks import build_pre_geocode, geocode, run_dbt

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


@task
def build_chicago_bike_theft_hotspots() -> str:
    return run_dbt("build", "--select", "geocoded_address_cache+,+chicago_bike_theft_hotspots")


@task_group
def build_bike_marts_with_geocoding(
    conn_id: str, task_logger: logging.Logger, restrict_region: str | None
) -> None:
    _pre_build = build_pre_geocode()
    _geocode = geocode(conn_id=conn_id, task_logger=task_logger, restrict_region=restrict_region)
    _build_theft_hotspots = build_chicago_bike_theft_hotspots()

    chain(_pre_build, _geocode, _build_theft_hotspots)


@task
def build_chicago_bike_parking() -> str:
    return run_dbt("build", "--select", "+chicago_bike_parking")


@task
def build_chicago_bike_crash_hotspots() -> str:
    return run_dbt("build", "--select", "+bike_crash_hotspots")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * 0",
    catchup=False,
    tags=["dbt"],
)
def refresh_bike_map_app():
    _build_geocoded_tables = build_bike_marts_with_geocoding(
        conn_id=CONN_ID,
        task_logger=task_logger,
        restrict_region="ST_MakeEnvelope(-87.94, 41.64, -87.52, 42.03, 4269)",
    )
    _build_parking_table = build_chicago_bike_parking()
    _build_crashes_table = build_chicago_bike_crash_hotspots()
    _export_data = export_bike_map_geojson(conn_id=CONN_ID, task_logger=task_logger)
    _deploy = deploy_bike_map(task_logger=task_logger)

    chain(
        [_build_geocoded_tables, _build_parking_table, _build_crashes_table], _export_data, _deploy
    )


refresh_bike_map_app()
