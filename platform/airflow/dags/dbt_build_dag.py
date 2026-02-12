from datetime import datetime
import subprocess

from airflow.sdk import dag, task


@dag(
    dag_id="dbt_build",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt"],
)
def dbt_build_dag():
    @task
    def dbt_build() -> str:
        result = subprocess.run(
            [
                "/home/airflow/dbt_venv/bin/dbt",
                "build",
                "--project-dir",
                "/opt/airflow/dbt",
                "--select",
                "chicago_homicide_and_non_fatal_shooting_victimizations",
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

    dbt_build()


dbt_build_dag()
