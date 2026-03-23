import logging
import subprocess


def run_dbt(
    *args: str,
    dbt_cmd: str = "/home/airflow/dbt_venv/bin/dbt",
    dbt_project_dir: str = "/opt/airflow/dbt",
    logger: logging.Logger | None = None,
) -> str:
    """Run a dbt command and return stdout. Raises on failure."""
    if logger is None:
        logger = logging.getLogger(__name__)
    process = subprocess.Popen(
        [dbt_cmd, *args, "--project-dir", dbt_project_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    output_lines = []
    for line in process.stdout:
        logger.info(line)  # prints to Airflow logs immediately
        output_lines.append(line)

    returncode = process.wait()
    if returncode != 0:
        msg = f"dbt build failed with exit code {returncode}\n{''.join(output_lines)}"
        logger.error(msg)
        raise Exception(msg)

    return "".join(output_lines)
