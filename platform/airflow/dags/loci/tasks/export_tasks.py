"""
Airflow tasks for exporting mart data to GeoJSON files.

Usage in a DAG:

    from loci.tasks.export_tasks import export_bike_map_geojson

    @dag(...)
    def my_dag():
        ...
        export_bike_map_geojson(conn_id="gis_dwh_db")
"""

from logging import Logger

from airflow.sdk import task
from loci.db.af_utils import get_postgres_engine
from loci.exports.geojson_export import BIKE_MAP_EXPORTS, GeoJsonExporter

BIKE_MAP_EXPORT_DIR = "/opt/airflow/exports/bike-map/data"


@task
def export_bike_map_geojson(conn_id: str, task_logger: Logger) -> list[str]:
    """Export bike map mart tables to GeoJSON files."""
    exporter = GeoJsonExporter(
        engine=get_postgres_engine(conn_id=conn_id, logger=task_logger),
        output_dir=BIKE_MAP_EXPORT_DIR,
    )
    paths = exporter.export_all(BIKE_MAP_EXPORTS)
    return [str(p) for p in paths]
