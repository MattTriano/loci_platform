"""
Airflow tasks for exporting mart data to GeoJSON files.

Usage in a DAG:

    from loci.tasks.export_tasks import export_bike_map_geojson

    @dag(...)
    def my_dag():
        ...
        export_bike_map_geojson(env="dev", conn_id="gis_dwh_db", task_logger=...)
"""

from logging import Logger
from pathlib import Path

from airflow.sdk import task
from loci.db.af_utils import get_postgres_engine
from loci.environments import get_env
from loci.exports.geojson_export import CITY_EXPORT_BUILDERS, GeoJsonExporter

BIKE_MAP_EXPORT_DIR_BASE = "/opt/airflow/exports/bike-map"


@task
def export_bike_map_geojson(env: str, city: str, conn_id: str, task_logger: Logger) -> list[str]:
    cfg = get_env(env, city)
    output_dir = Path(BIKE_MAP_EXPORT_DIR_BASE) / env / city / "data"

    exporter = GeoJsonExporter(
        engine=get_postgres_engine(conn_id=conn_id, logger=task_logger),
        output_dir=output_dir,
    )
    builder = CITY_EXPORT_BUILDERS[city]
    configs = builder(marts_schema=cfg.marts_schema)
    paths = exporter.export_all(configs)
    return [str(p) for p in paths]
