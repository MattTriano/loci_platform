# loci_platform/platform/airflow/dags/dag_files/refresh_bike_map_dc.py
"""Refresh the Washington DC bike map: dbt build → export → deploy."""

from datetime import datetime

from airflow.sdk import dag
from loci.exports.bike_map_layers import LayerDisplayConfig, PopupField
from loci.exports.geojson_export import GeoJSONExportConfig
from loci.sources.dataset_specs import WASHINGTON_DC_BBOX
from loci.tasks.bike_map_tasks import (
    CityBuildSpec,
    build_refresh_task_graph,
    standard_dag_params,
)
from loci.tasks.testing.route_tests import RouteTestCase

DC_GEOJSON_EXPORTS: list[GeoJSONExportConfig] = [
    GeoJSONExportConfig(
        name="thefts",
        table="dc_bikeindex_bike_thefts",
        latitude_column="latitude",
        longitude_column="longitude",
        properties=[
            "source",
            "source_id",
            "theft_date",
            "theft_year",
            "theft_hour",
            "bike_title",
            "bike_description",
            "theft_description",
            "locking_description",
            "lock_defeat_description",
            "theft_status",
            "latitude",
            "longitude",
            "location",
        ],
    ),
    GeoJSONExportConfig(
        name="parking",
        table="dc_osm_bike_parking",
        geometry_column="geom",
        properties=[
            "osm_id",
            "type",
            "capacity",
            "covered",
            "indoor",
            "fee",
            "lit",
            "operator",
            "access",
        ],
    ),
]

DC_LAYER_DISPLAYS: list[LayerDisplayConfig] = [
    LayerDisplayConfig(
        export_name="thefts",
        label="Thefts",
        color="#f59e0b",
        popup_title="Thefts",
        date_field="theft_date",
        popup_fields=[
            PopupField(key="source", label="Source"),
            PopupField(key="theft_date", label="Theft Date"),
            PopupField(key="theft_year", label="Theft Year"),
            PopupField(key="theft_hour", label="Theft Hour", fmt="hour"),
            PopupField(key="bike_title", label="Bike Title"),
            PopupField(key="bike_description", label="Bike Description"),
            PopupField(key="theft_description", label="Theft Description"),
            PopupField(key="locking_description", label="Locking Description"),
            PopupField(key="lock_defeat_description", label="Lock Defeat Description"),
            PopupField(key="theft_status", label="Theft Status"),
        ],
    ),
    LayerDisplayConfig(
        export_name="parking",
        label="Parking",
        color="#22c55e",
        popup_title="Parking",
        popup_fields=[
            PopupField(key="type", label="Type"),
            PopupField(key="capacity", label="Capacity"),
            PopupField(key="covered", label="Covered", fmt="yes_no_bool"),
            PopupField(key="indoor", label="Indoor", fmt="yes_no_bool"),
            PopupField(key="lit", label="Lit", fmt="yes_no_bool"),
            PopupField(key="fee", label="Fee", fmt="yes_no_bool"),
            PopupField(key="operator", label="Operator"),
            PopupField(key="access", label="Access"),
        ],
    ),
]

DC_ROUTE_TESTS: list[RouteTestCase] = []

DC_SPEC = CityBuildSpec(
    city="dc",
    bbox=WASHINGTON_DC_BBOX,
    pre_export_dbt_selects=[
        ("--select", "+dc_bikeindex_bike_thefts"),
        ("--select", "+dc_osm_bike_parking"),
    ],
    geojson_exports=DC_GEOJSON_EXPORTS,
    layer_displays=DC_LAYER_DISPLAYS,
    weights_dbt_select="+dc_bike_stress_weighted_segments",
    route_tests=DC_ROUTE_TESTS,
)


@dag(
    dag_id="refresh_bike_map_dc",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bike-map", "dc"],
    params=standard_dag_params("dc"),
)
def refresh_bike_map_dc():
    build_refresh_task_graph(DC_SPEC)


refresh_bike_map_dc()
