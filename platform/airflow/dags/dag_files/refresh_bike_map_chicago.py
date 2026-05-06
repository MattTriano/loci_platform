# loci_platform/platform/airflow/dags/dag_files/refresh_bike_map_chicago.py
"""Refresh the Chicago bike map: dbt build → export → deploy."""

from datetime import datetime

from airflow.sdk import dag
from loci.exports.geojson_export import GeoJSONExportConfig
from loci.sources.dataset_specs import CHICAGO_BBOX
from loci.tasks.bike_map_tasks import (
    CityBuildSpec,
    build_refresh_task_graph,
    standard_dag_params,
)

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
        table="chicago_bike_parking",
        geometry_column="geom",
        properties=[
            "source",
            "id",
            "location",
            "name",
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

CHICAGO_SPEC = CityBuildSpec(
    city="chicago",
    bbox=CHICAGO_BBOX,
    pre_export_dbt_selects=[
        # Bike theft hotspots — depends on geocoded address cache.
        (
            "--select",
            "geocoded_address_cache+,stg__chicago_bikeindex_bike_thefts,+chicago_bike_theft_hotspots stg__chicago_cpd_bike_thefts",
        ),
        # Bike parking.
        ("--select", "+chicago_bike_parking"),
        # Bike crash hotspots — must use cautious indirect selection to
        # skip the compare_aggregations test on chicago_bike_stress_weighted_edges,
        # which references chicago_bike_crash_hotspots but isn't built yet.
        ("--select", "+chicago_bike_crash_hotspots", "--indirect-selection=cautious"),
    ],
    geojson_exports=CHICAGO_GEOJSON_EXPORTS,
    weights_dbt_select="+chicago_bike_stress_weighted_segments",
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
