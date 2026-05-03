# loci_platform/platform/airflow/dags/dag_files/refresh_bike_map_chicago.py
"""Refresh the Chicago bike map: dbt build → export → deploy."""

from datetime import datetime

from airflow.sdk import dag
from loci.geo import BBox
from loci.tasks.bike_map_tasks import (
    CityBuildSpec,
    build_refresh_task_graph,
    standard_dag_params,
)

CHICAGO_SPEC = CityBuildSpec(
    city="chicago",
    bbox=BBox(south=41.64, west=-87.94, north=42.03, east=-87.52),
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
    weights_dbt_select="+chicago_bike_stress_weighted_edges",
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
