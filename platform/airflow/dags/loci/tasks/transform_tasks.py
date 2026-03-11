import subprocess
from logging import Logger

from airflow.sdk import task, task_group
from airflow.sdk.bases.operator import chain

DBT_CMD = "/home/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"


def run_dbt(*args: str) -> str:
    """Run a dbt command and return stdout. Raises on failure."""
    result = subprocess.run(
        [DBT_CMD, *args, "--project-dir", DBT_PROJECT_DIR],
        capture_output=True,
        text=True,
    )
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
def build_pre_geocode() -> str:
    return run_dbt("build", "--select", "+geocoded_address_cache")


@task
def geocode(conn_id: str, task_logger: Logger, restrict_region: str | None) -> dict:
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
def build_post_geocode() -> str:
    return run_dbt("build", "--select", "geocoded_address_cache+")


@task_group
def dbt_build_with_geocoding(
    conn_id: str, task_logger: Logger, restrict_region: str | None
) -> None:
    _pre_build = build_pre_geocode()
    _geocode = geocode(conn_id=conn_id, task_logger=task_logger, restrict_region=restrict_region)
    _post_build = build_post_geocode()

    chain(_pre_build, _geocode, _post_build)
