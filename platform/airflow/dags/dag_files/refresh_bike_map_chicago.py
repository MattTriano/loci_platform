# loci_platform/platform/airflow/dags/dag_files/refresh_bike_map_chicago.py
"""Refresh the Chicago bike map: dbt build → export → deploy."""

from datetime import datetime

from airflow.sdk import dag
from loci.exports.bike_map_layers import LayerDisplayConfig, PopupField
from loci.exports.geojson_export import GeoJSONExportConfig
from loci.sources.dataset_specs import CHICAGO_BBOX
from loci.tasks.bike_map_tasks import (
    CityBuildSpec,
    build_refresh_task_graph,
    standard_dag_params,
)
from loci.tasks.testing.route_tests import RouteTestCase

CHICAGO_GEOJSON_EXPORTS = [
    GeoJSONExportConfig(
        name="crashes",
        table="chicago_bike_crash_hotspots",
        geometry_column="geom",
        properties=[
            "crash_record_id",
            "crash_date",
            "local_crash_time",
            "local_crash_day_of_week",
            "first_crash_type",
            "most_severe_injury",
            "hit_and_run_i",
            "dooring_i",
            "weather_condition",
            "lighting_condition",
            "street_name",
            "street_direction",
            "prim_contributory_cause",
            "injuries_total",
            "injuries_fatal",
            "injuries_incapacitating",
            "severity_score",
            "crash_year",
        ],
    ),
    GeoJSONExportConfig(
        name="thefts",
        table="chicago_bike_theft_hotspots",
        latitude_column="latitude",
        longitude_column="longitude",
        properties=[
            "source",
            "source_id",
            "theft_date",
            "theft_year",
            "theft_hour",
            "location_description",
            "bike_description",
            "theft_description",
            "locking_description",
        ],
    ),
    GeoJSONExportConfig(
        name="parking",
        table="chicago_osm_bike_parking",
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

CHICAGO_LAYER_DISPLAYS: list[LayerDisplayConfig] = [
    LayerDisplayConfig(
        export_name="crashes",
        label="Bike Crashes",
        color="#ef4444",
        popup_title="Bike Crash",
        date_field="crash_date",
        popup_fields=[
            PopupField(key="crash_date", label="Date"),
            PopupField(key="local_crash_time", label="Time"),
            PopupField(key="local_crash_day_of_week", label="Day"),
            # The street_with_direction formatter combines street_direction
            # + street_name; key=street_name drives "is this row shown".
            PopupField(key="street_name", label="Street", fmt="street_with_direction"),
            PopupField(key="first_crash_type", label="Type"),
            PopupField(key="most_severe_injury", label="Worst Injury"),
            PopupField(key="severity_score", label="Severity"),
            PopupField(key="prim_contributory_cause", label="Cause"),
            PopupField(key="weather_condition", label="Weather"),
            PopupField(key="lighting_condition", label="Lighting"),
            PopupField(key="hit_and_run_i", label="Hit & Run", fmt="yes_no_yn"),
            PopupField(key="dooring_i", label="Dooring", fmt="yes_no_yn"),
        ],
    ),
    LayerDisplayConfig(
        export_name="thefts",
        label="Bike Thefts",
        color="#f59e0b",
        popup_title="Bike Theft",
        date_field="theft_date",
        popup_fields=[
            PopupField(key="source", label="Source"),
            PopupField(key="theft_date", label="Date"),
            PopupField(key="theft_hour", label="Hour", fmt="hour"),
            PopupField(key="location_description", label="Location"),
            PopupField(key="bike_description", label="Bike"),
            PopupField(key="theft_description", label="Details"),
            PopupField(key="locking_description", label="Locking"),
        ],
    ),
    LayerDisplayConfig(
        export_name="parking",
        label="Bike Parking",
        color="#22c55e",
        popup_title="Bike Parking",
        # Was 40 in the original hardcoded JS; others used 45.
        cluster_radius=40,
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


CHICAGO_ROUTE_TESTS: list[RouteTestCase] = [
    # -- Lakefront Trail should be preferred over Lake Shore Drive --
    RouteTestCase(
        name="Uptown to Museum Campus should use Lakefront Trail, not LSD",
        origin=(41.9660, -87.6465),  # Montrose Harbor area
        destination=(41.8665, -87.6070),  # Museum Campus / Shedd area
        must_use=["Lakefront Trail"],
        must_avoid=["Lake Shore Drive", "DuSable Lake Shore Drive"],
    ),
    RouteTestCase(
        name="Edgewater to Hyde Park along lake should use Lakefront Trail",
        origin=(41.9835, -87.6500),  # Edgewater, near the lake
        destination=(41.7945, -87.5805),  # Hyde Park, near the lake
        must_use=["Lakefront Trail"],
        must_avoid=["Lake Shore Drive", "DuSable Lake Shore Drive"],
    ),
    # -- Bloomingdale Trail (the 606) --
    RouteTestCase(
        name="Humboldt Park to Bucktown along 606 corridor should use Bloomingdale Trail",
        origin=(41.9155, -87.7198),  # west end of 606
        destination=(41.9140, -87.6680),  # east end of 606
        must_use=["Bloomingdale Trail"],
    ),
    # -- General: never route onto high-speed arterials --
    RouteTestCase(
        name="Wicker Park to Logan Square should avoid Western Ave",
        origin=(41.9085, -87.6796),  # Wicker Park
        destination=(41.9295, -87.7080),  # Logan Square monument
        must_avoid=["Western Avenue", "North Western Avenue"],
    ),
    RouteTestCase(
        name="Belmont under the Kennedy should just use the cycleway",
        origin=(41.9394, -87.7117),  # West of Kennedy
        destination=(41.9393, -87.7051),  # East of Kennedy
        must_use=["Belmont Avenue Bikeway"],
        must_avoid=["North Avondale Avenue", "North Kedzie Avenue"],
    ),
    RouteTestCase(
        name="Avoid the northbound underpass on Ashland above Cortland",
        origin=(41.9157, -87.6678),  # South of the underpass
        destination=(41.9189, -87.6682),  # North of the underpass
        must_use=["West Cortland Street", "North Elston Avenue"],
    ),
]

CHICAGO_SPEC = CityBuildSpec(
    city="chicago",
    bbox=CHICAGO_BBOX,
    pre_export_dbt_selects=[
        # Bike theft hotspots — depends on geocoded address cache.
        ("--select", "+chicago_bike_theft_hotspots"),
        # Bike parking.
        ("--select", "+chicago_osm_bike_parking"),
        # Bike crash hotspots — must use cautious indirect selection to
        # skip the compare_aggregations test on chicago_bike_stress_weighted_edges,
        # which references chicago_bike_crash_hotspots but isn't built yet.
        ("--select", "+chicago_bike_crash_hotspots", "--indirect-selection=cautious"),
    ],
    geojson_exports=CHICAGO_GEOJSON_EXPORTS,
    # layer_displays=CHICAGO_LAYER_DISPLAYS,
    weights_dbt_select="+chicago_bike_stress_weighted_segments",
    route_tests=CHICAGO_ROUTE_TESTS,
)


@dag(
    dag_id="refresh_bike_map_chicago",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bike-map", "chicago"],
    params=standard_dag_params("chicago"),
)
def refresh_bike_map_chicago():
    build_refresh_task_graph(CHICAGO_SPEC)


refresh_bike_map_chicago()
