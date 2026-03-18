import subprocess
from datetime import datetime

from airflow.sdk import dag, task

@task
def alt_build() -> str:
    import subprocess

    process = subprocess.Popen(
        [
            "/home/airflow/dbt_venv/bin/dbt",
            "build",
            "--project-dir",
            "/opt/airflow/dbt",
            "--select",
            "bike_safety_weighted_edges",
            "--threads",
            "1"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    
    output_lines = []
    for line in process.stdout:
        print(line, end="")  # prints to Airflow logs immediately
        output_lines.append(line)
    
    returncode = process.wait()
    if returncode != 0:
        raise Exception(
            f"dbt build failed with exit code {returncode}\n{''.join(output_lines)}"
        )

    return "".join(output_lines)

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
    alt_build()


dbt_build_dag()
