from logging import Logger

from airflow.sdk import task, task_group
from airflow.sdk.bases.operator import chain
from loci.collectors.tiger.collector import TigerCollector
from loci.collectors.tiger.spec import TigerDatasetSpec
from loci.db.af_utils import get_postgres_engine
from loci.sources.update_configs import DatasetUpdateConfig
from loci.tasks.task_utils import check_ingestion_log, choose_update_mode
from loci.tracking.ingestion_tracker import IngestionTracker


def _get_collector(conn_id: str, task_logger: Logger) -> TigerCollector:
    pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    tracker = IngestionTracker(engine=pg_engine)
    return TigerCollector(engine=pg_engine, tracker=tracker)


@task
def run_full_update(
    spec: TigerDatasetSpec, update_config: DatasetUpdateConfig, conn_id: str, task_logger: Logger
) -> bool:
    collector = _get_collector(conn_id, task_logger)
    summary_results = collector.collect(spec=spec, force=True)
    task_logger.info("Collection summary", extra={"payload": summary_results})
    return True


@task
def run_incremental_update(
    spec: TigerDatasetSpec, update_config: DatasetUpdateConfig, conn_id: str, task_logger: Logger
) -> bool:
    collector = _get_collector(conn_id, task_logger)
    summary_results = collector.collect(spec=spec, force=False)
    task_logger.info("Collection summary", extra={"payload": summary_results})
    return True


@task_group
def update_tiger_table(
    dataset_spec: TigerDatasetSpec,
    update_config: DatasetUpdateConfig,
    conn_id: str,
    task_logger: Logger,
) -> None:
    _update_mode = choose_update_mode(update_config=update_config, task_logger=task_logger)
    _full_update = run_full_update(
        conn_id=conn_id, spec=dataset_spec, update_config=update_config, task_logger=task_logger
    )
    _incremental_update = run_incremental_update(
        conn_id=conn_id, spec=dataset_spec, update_config=update_config, task_logger=task_logger
    )
    _check_ingestion_log = check_ingestion_log(
        conn_id=conn_id, update_config=update_config, task_logger=task_logger
    )

    chain(_update_mode, _full_update, _check_ingestion_log)
    chain(_update_mode, _incremental_update, _check_ingestion_log)
