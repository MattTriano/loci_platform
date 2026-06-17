# loci_platform/platform/airflow/dags/dag_files/refresh_bike_map_portland.py
"""Refresh the Portland bike map: dbt build → export → deploy."""

from datetime import datetime

from airflow.sdk import dag
from loci.apps.bike_map import data_layers
from loci.exports.bike_map_layers import LayerDisplayConfig
from loci.exports.geojson_export import GeoJSONExportConfig
from loci.sources.dataset_specs import PORTLAND_BBOX
from loci.tasks.bike_map_tasks import (
    CityBuildSpec,
    build_refresh_task_graph,
    standard_dag_params,
)
from loci.tasks.testing.route_tests import Gate, RouteTestCase

CITY = "portland"

CITY_GEOJSON_EXPORTS: list[GeoJSONExportConfig] = [
    data_layers.bikeindex_bike_theft_geojson_config_factory(CITY),
    data_layers.osm_bike_parking_geojson_config_factory(CITY),
]

CITY_LAYER_DISPLAYS: list[LayerDisplayConfig] = [
    data_layers.BIKEINDEX_BIKE_THEFTS_LAYER_CONFIG,
    data_layers.OSM_BIKE_PARKING_LAYER_CONFIG,
]

CITY_ROUTE_TESTS: list[RouteTestCase] = [
    RouteTestCase(
        name="Avoids hilly variations when largely flat alt routes are available",
        origin=(45.5184, -122.6983),
        destination=(45.5089, -122.6950),
        must_not_cross=[
            Gate((45.5129, -122.6965), (45.5130, -122.6955)),
            Gate((45.5114, -122.6965), (45.5120, -122.6961)),
        ],
    ),
]

ROUTABLE_ORIGIN_DEST_PAIR = ((45.5165, -122.6832), (45.5233, -122.6816))

CITY_SPEC = CityBuildSpec(
    city=CITY,
    bbox=PORTLAND_BBOX,
    pre_export_dbt_selects=[
        ("--select", f"+{CITY}_bikeindex_bike_thefts"),
        ("--select", f"+{CITY}_osm_bike_parking"),
    ],
    geojson_exports=CITY_GEOJSON_EXPORTS,
    layer_displays=CITY_LAYER_DISPLAYS,
    weights_dbt_select=f"+{CITY}_bike_stress_weighted_segments",
    route_tests=CITY_ROUTE_TESTS,
    synthetic_check_fixture=ROUTABLE_ORIGIN_DEST_PAIR,
)


@dag(
    dag_id=f"refresh_bike_map_{CITY}",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bike-map", CITY],
    params=standard_dag_params(CITY),
)
def refresh_bike_map_portland():
    build_refresh_task_graph(CITY_SPEC)


refresh_bike_map_portland()
