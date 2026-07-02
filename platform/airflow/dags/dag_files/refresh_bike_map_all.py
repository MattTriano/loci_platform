# loci_platform/platform/airflow/dags/dag_files/refresh_bike_map_all.py
"""Controller DAG: fan out to every per-city refresh_bike_map_<city> DAG.

Triggered manually. Runs `dbt deps` once for the whole fan-out, then triggers
each per-city DAG in parallel (capped by max_active_tasks). Each trigger task
waits for its child to finish, so the controller's grid view shows one
green/red square per city. One city failing does not stop the others.

`dbt deps` is run once here, rather than 10x by the children, because dbt
wipes and reinstalls `dbt_packages/` on each `deps` invocation, and that
directory is shared across the whole dbt project — concurrent children
racing on `deps` would yank packages out from under each other's `build`
steps. Children skip their own `install_dependencies` via skip_deps=True.
"""

from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task
from loci.environments import VALID_ENVS
from loci.tasks.transform_tasks import run_dbt

CITIES = [
    "boston",
    "chicago",
    "dc",
    "denver",
    "detroit",
    "madison",
    "nola",
    "nyc",
    "portland",
    "sf",
    "toronto",
]


@task
def install_dependencies_once() -> str:
    return run_dbt("deps")


@dag(
    dag_id="refresh_bike_map_all",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,
    tags=["bike-map", "controller"],
    params={
        "env": Param(
            "dev",
            type="string",
            enum=list(VALID_ENVS),
            title="Target environment",
            description="Which AWS account + dbt target to build and deploy to.",
        ),
    },
)
def refresh_bike_map_all():
    deps = install_dependencies_once()

    for city in CITIES:
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{city}",
            trigger_dag_id=f"refresh_bike_map_{city}",
            conf={"env": "{{ params.env }}", "skip_deps": True},
            wait_for_completion=True,
            failed_states=["failed"],
        )
        deps >> trigger


refresh_bike_map_all()
