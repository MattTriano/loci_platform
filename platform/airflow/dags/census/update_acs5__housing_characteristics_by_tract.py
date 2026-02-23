import datetime as dt

# import logging
import os
from logging import Logger, getLogger

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain

from collectors.census.client import CensusClient
from collectors.census.collector import CensusCollector
from collectors.census.spec import CensusDatasetSpec
from db.af_utils import get_postgres_engine
from sources.dataset_specs import ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_SPEC as DATASET_SPEC
from sources.update_configs import (
    ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_UPDATE_CONFIG as UPDATE_CONFIG,
)
from sources.update_configs import DatasetUpdateConfig
from tracking.ingestion_tracker import IngestionTracker

task_logger = getLogger("airflow.task")


CONN_ID = "gis_dwh_db"


def _get_collector(conn_id: str, task_logger: Logger) -> CensusCollector:
    pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    client = CensusClient(api_key=os.environ["CENSUS_API_KEY"])
    tracker = IngestionTracker(engine=pg_engine)
    return CensusCollector(
        engine=pg_engine,
        client=client,
        tracker=tracker,
    )


@task
def run_full_update(
    conn_id: str, spec: CensusDatasetSpec, update_config: DatasetUpdateConfig, task_logger: Logger
) -> bool:
    collector = _get_collector(conn_id, task_logger)
    summary_results = collector.collect(spec=spec, force=True)
    task_logger.info("Collection summary", extra={"payload": summary_results})


@dag(
    schedule=UPDATE_CONFIG.update_cron,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["census", "update", "housing", "tract"],
)
def update_acs5__housing_characteristics_by_tract():
    full_run = run_full_update(
        conn_id=CONN_ID, spec=DATASET_SPEC, update_config=UPDATE_CONFIG, task_logger=task_logger
    )

    chain(full_run)


update_acs5__housing_characteristics_by_tract()
