# loci_platform/platform/airflow/dags/dag_files/bike_map/refresh_bike_map_landing.py
"""Refresh the bike-map landing page: deploy static site + injected config to S3."""

import logging
from datetime import datetime

from airflow.sdk import Param, dag
from loci.environments import VALID_ENVS
from loci.tasks.deploy_tasks import deploy_bike_map_landing

ENV_TEMPLATE = "{{ params.env }}"

task_logger = logging.getLogger("airflow.task")


@dag(
    dag_id="refresh_bike_map_landing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bike-map", "landing"],
    params={
        "env": Param(
            "dev",
            type="string",
            enum=list(VALID_ENVS),
            title="Target environment",
            description="Which AWS account to deploy the landing page to.",
        ),
    },
)
def refresh_bike_map_landing():
    deploy_bike_map_landing(env=ENV_TEMPLATE, task_logger=task_logger)


refresh_bike_map_landing()
