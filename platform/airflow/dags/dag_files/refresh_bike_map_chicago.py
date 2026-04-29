"""Refresh the Chicago bike map: dbt build → export → deploy."""

from datetime import datetime

from airflow.sdk import dag
from loci.tasks.bike_map_tasks import (
    CityBuildSpec,
    build_refresh_task_graph,
    standard_dag_params,
)

CHICAGO_SPEC = CityBuildSpec(
    city="chicago",
    geocode_bbox="ST_MakeEnvelope(-87.94, 41.64, -87.52, 42.03, 4269)",
    pre_export_dbt_selects=[
        # Bike theft hotspots — depends on geocoded address cache.
        (
            "--select",
            "geocoded_address_cache+,+chicago_bike_theft_hotspots stg__bikeindex_chicago_stolen_bikes",
        ),
        # Bike parking.
        ("--select", "+chicago_bike_parking"),
        # Bike crash hotspots — must use cautious indirect selection to
        # skip the compare_aggregations test on bike_safety_weighted_edges,
        # which references bike_crash_hotspots but isn't built yet.
        ("--select", "+bike_crash_hotspots", "--indirect-selection=cautious"),
    ],
    weights_dbt_select="+bike_safety_weighted_edges",
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
