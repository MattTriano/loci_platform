import subprocess
from datetime import datetime

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain
from loci.transform.utils import run_dbt


@task
def install_dependencies() -> str:
    return run_dbt("deps")


@task
def alt_build() -> str:
    return run_dbt("build", "--select", "+detroit_bike_stress_weighted_segments")


@dag(
    dag_id="dbt_build",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt"],
)
def dbt_build_dag():
    _deps = install_dependencies()
    _build = alt_build()

    chain(_deps, _build)


dbt_build_dag()
