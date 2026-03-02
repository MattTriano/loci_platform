import os
from logging import Logger

from airflow.models.taskinstance import TaskInstance
from airflow.sdk import task, task_group
from airflow.sdk.bases.operator import chain
from loci.collectors.config import IncrementalConfig
from loci.collectors.socrata.collector import SocrataCollector
from loci.db.af_utils import get_postgres_engine
from loci.sources.update_configs import DatasetUpdateConfig
from loci.tasks.task_utils import check_ingestion_log, choose_update_mode
from loci.tracking.ingestion_tracker import IngestionTracker


def get_task_group_id_prefix(task_instance: TaskInstance) -> str:
    task_id_parts = task_instance.task_id.split(".")
    if len(task_id_parts) > 1:
        return ".".join(task_id_parts[:-1]) + "."
    else:
        return ""


def _get_collector(conn_id: str, task_logger: Logger) -> SocrataCollector:
    pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    tracker = IngestionTracker(engine=pg_engine)
    return SocrataCollector(
        engine=pg_engine,
        tracker=tracker,
        app_token=os.environ["SOCRATA_APP_TOKEN"],
    )


@task
def run_full_update(conn_id: str, update_config: DatasetUpdateConfig, task_logger: Logger) -> bool:
    collector = _get_collector(conn_id, task_logger)
    rows = collector.full_refresh(
        dataset_id=update_config.spec.dataset_id,
        target_table=update_config.spec.target_table,
        target_schema="raw_data",
        config=update_config,
    )
    task_logger.info(
        f"Full refresh: ingested {rows} rows into raw_data.{update_config.spec.target_table}"
    )
    return True


@task
def run_incremental_update(
    conn_id: str, update_config: DatasetUpdateConfig, task_logger: Logger
) -> bool:
    collector = _get_collector(conn_id, task_logger)
    rows = collector.incremental_update(
        dataset_id=update_config.spec.dataset_id,
        target_table=update_config.spec.target_table,
        target_schema="raw_data",
        config=IncrementalConfig(
            incremental_column=":updated_at",
            entity_key=update_config.spec.entity_key or None,
        ),
        entity_key=update_config.spec.entity_key or None,
    )
    task_logger.info(
        f"Incremental: ingested {rows} rows into raw_data.{update_config.spec.target_table}"
    )
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
