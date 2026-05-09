# loci_platform/platform/airflow/dags/dag_files/refresh_bike_map_chicago.py
"""Refresh the Chicago bike map: dbt build → export → deploy."""

from datetime import datetime

from airflow.sdk import dag
from loci.exports.bike_map_layers import LayerDisplayConfig
from loci.exports.geojson_export import GeoJSONExportConfig
from loci.sources.dataset_specs import DETROIT_BBOX
from loci.tasks.bike_map_tasks import (
    CityBuildSpec,
    build_refresh_task_graph,
    standard_dag_params,
)
from loci.tasks.testing.route_tests import RouteTestCase

DETROIT_GEOJSON_EXPORTS: list[GeoJSONExportConfig] = []

DETROIT_LAYER_DISPLAYS: list[LayerDisplayConfig] = []

DETROIT_ROUTE_TESTS: list[RouteTestCase] = []

DETROIT_SPEC = CityBuildSpec(
    city="detroit",
    bbox=DETROIT_BBOX,
    pre_export_dbt_selects=[],
    geojson_exports=DETROIT_GEOJSON_EXPORTS,
    layer_displays=DETROIT_LAYER_DISPLAYS,
    weights_dbt_select="+detroit_bike_stress_weighted_segments",
    route_tests=DETROIT_ROUTE_TESTS,
)


@dag(
    dag_id="refresh_bike_map_detroit",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bike-map", "detroit"],
    params=standard_dag_params("detroit"),
)
def refresh_bike_map_detroit():
    build_refresh_task_graph(DETROIT_SPEC)


refresh_bike_map_detroit()
