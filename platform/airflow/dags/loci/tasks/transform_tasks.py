# loci_platform/platform/airflow/dags/loci/tasks/transform_tasks.py
import subprocess
from logging import Logger

from airflow.sdk import task, task_group
from airflow.sdk.bases.operator import chain
from loci.environments import get_env

DBT_CMD = "/home/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"


def run_dbt(*args: str, target: str | None = None) -> str:
    """Run a dbt command and return stdout. Raises on failure.

    If target is provided, passes --target to dbt so the right
    profiles.yml output is used.
    """
    cmd = [DBT_CMD, *args, "--project-dir", DBT_PROJECT_DIR]
    if target:
        cmd += ["--target", target]

    result = subprocess.run(cmd, capture_output=True, text=True)
    print("=== STDOUT ===")
    print(result.stdout)
    print("=== STDERR ===")
    print(result.stderr)
    if result.returncode != 0:
        raise Exception(
            f"dbt failed with exit code {result.returncode}\n{result.stderr}\n{result.stdout}"
        )
    return result.stdout


@task
def build_pre_geocode(env: str, city: str) -> str:
    target = get_env(env, city).dbt_target
    return run_dbt("build", "--select", "+geocoded_address_cache", target=target)


@task
def geocode(
    conn_id: str,
    task_logger: Logger,
    restrict_region: tuple[float, float, float, float] | None,
) -> dict:
    from loci.db.af_utils import get_postgres_engine
    from loci.transform.geocode import TigerGeocoder

    engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    geocoder = TigerGeocoder(
        engine=engine,
        schema_name="staging",
        restrict_region=restrict_region,
    )
    return geocoder.process_all()


@task
def build_post_geocode(env: str, city: str) -> str:
    target = get_env(env, city).dbt_target
    return run_dbt("build", "--select", "geocoded_address_cache+", target=target)


@task_group
def dbt_build_with_geocoding(
    env: str, conn_id: str, task_logger: Logger, restrict_region: tuple[float, float, float, float]
) -> None:
    _pre_build = build_pre_geocode(env=env, city="{{ params.city }}")
    _geocode = geocode(conn_id=conn_id, task_logger=task_logger, restrict_region=restrict_region)
    _post_build = build_post_geocode(env=env, city="{{ params.city }}")

    chain(_pre_build, _geocode, _post_build)
