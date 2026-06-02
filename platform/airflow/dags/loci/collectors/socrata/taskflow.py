# /loci_platform/platform/airflow/dags/loci/collectors/socrata/taskflow.py
import os
from logging import Logger

from airflow.sdk import task, task_group
from airflow.sdk.bases.operator import chain
from loci.collectors.socrata.collector import SocrataCollector
from loci.db.af_utils import get_postgres_engine
from loci.sources.update_configs import DatasetUpdateConfig
from loci.tasks.task_utils import check_ingestion_log, choose_update_mode
from loci.tracking.ingestion_tracker import IngestionTracker


def _get_collector(conn_id: str, task_logger: Logger) -> SocrataCollector:
    pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    tracker = IngestionTracker(engine=pg_engine)
    return SocrataCollector(
        engine=pg_engine,
        tracker=tracker,
        app_token=os.environ["SOCRATA_APP_TOKEN"],
    )


@task
def run_full_update(update_config: DatasetUpdateConfig, conn_id: str, task_logger: Logger) -> bool:
    collector = _get_collector(conn_id, task_logger)
    summary_results = collector.collect(spec=update_config.spec, force=True)
    task_logger.info("Collection summary", extra={"payload": summary_results})
    return True


@task
def run_incremental_update(
    update_config: DatasetUpdateConfig, conn_id: str, task_logger: Logger
) -> bool:
    collector = _get_collector(conn_id, task_logger)
    summary_results = collector.collect(spec=update_config.spec, force=False)
    task_logger.info("Collection summary", extra={"payload": summary_results})
    return True


@task_group
def update_socrata_table(
    update_config: DatasetUpdateConfig, conn_id: str, task_logger: Logger
) -> None:
    _update_mode = choose_update_mode(update_config=update_config, task_logger=task_logger)
    _full_update = run_full_update(
        conn_id=conn_id, update_config=update_config, task_logger=task_logger
    )
    _incremental_update = run_incremental_update(
        conn_id=conn_id, update_config=update_config, task_logger=task_logger
    )
    _check_ingestion_log = check_ingestion_log(
        conn_id=conn_id, update_config=update_config, task_logger=task_logger
    )

    chain(_update_mode, _full_update, _check_ingestion_log)
    chain(_update_mode, _incremental_update, _check_ingestion_log)
