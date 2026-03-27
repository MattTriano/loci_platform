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
    return run_dbt("build", "--select", "+bike_safety_weighted_edges")


@task
def dbt_build() -> str:
    result = subprocess.run(
        [
            "/home/airflow/dbt_venv/bin/dbt",
            "build",
            "--project-dir",
            "/opt/airflow/dbt",
            "--select",
            "+bike_safety_weighted_edges",
        ],
        capture_output=True,
        text=True,
    )

    print("=== STDOUT ===")
    print(result.stdout)
    print("=== STDERR ===")
    print(result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"dbt build failed with exit code {result.returncode}\n{result.stderr}\n{result.stdout}"
        )

    return result.stdout


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
