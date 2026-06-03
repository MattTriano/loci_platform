# /loci_platform/platform/airflow/dags/loci/collectors/ckan/taskflow.py
from logging import Logger

from airflow.sdk import task, task_group
from airflow.sdk.bases.operator import chain
from loci.collectors.ckan.collector import CKANCollector
from loci.db.af_utils import get_postgres_engine
from loci.sources.update_configs import DatasetUpdateConfig
from loci.tasks.task_utils import check_ingestion_log
from loci.tracking.ingestion_tracker import IngestionTracker


def _get_collector(conn_id: str, task_logger: Logger) -> CKANCollector:
    pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    tracker = IngestionTracker(engine=pg_engine)
    return CKANCollector(engine=pg_engine, logger=task_logger, tracker=tracker)


@task
def run_full_update(update_config: DatasetUpdateConfig, conn_id: str, task_logger: Logger) -> bool:
    collector = _get_collector(conn_id, task_logger)
    summary_results = collector.collect(spec=update_config.spec, force=True)
    task_logger.info("Collection summary", extra={"payload": summary_results})
    return True


@task_group
def update_ckan_table(
    update_config: DatasetUpdateConfig,
    conn_id: str,
    task_logger: Logger,
) -> None:
    # CKAN is full-refresh-only: there is no incremental mode to branch to,
    # so this task group stays single-path (no choose_update_mode), unlike
    # the other sources. This is a deliberate divergence, not an oversight.
    _full_update = run_full_update(
        conn_id=conn_id, update_config=update_config, task_logger=task_logger
    )
    _check_ingestion_log = check_ingestion_log(
        conn_id=conn_id, update_config=update_config, task_logger=task_logger
    )

    chain(_full_update, _check_ingestion_log)
