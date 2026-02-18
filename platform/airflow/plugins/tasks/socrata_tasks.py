import os
from logging import Logger

import pendulum
from airflow.models.taskinstance import TaskInstance
from airflow.sdk import task, task_group, get_current_context
from airflow.sdk.bases.operator import chain
from airflow.task.trigger_rule import TriggerRule
from croniter import croniter

from db.af_utils import get_postgres_engine
from collectors.socrata import SocrataCollector, IncrementalConfig
from collectors.ingestion_tracker import IngestionTracker
from sources.update_configs import DatasetUpdateConfig


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


@task.branch()
def choose_update_mode(update_config: DatasetUpdateConfig, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    logical_date = context["logical_date"]
    cron = croniter(update_config.full_update_cron, logical_date.subtract(seconds=1))
    next_hit = pendulum.instance(cron.get_next(pendulum.DateTime))
    task_logger.info(f"tg_id_prefix: {tg_id_prefix}")
    if next_hit.date() == logical_date.date():
        return f"{tg_id_prefix}run_full_update"
    return f"{tg_id_prefix}run_incremental_update"


@task
def run_full_update(
    conn_id: str, update_config: DatasetUpdateConfig, task_logger: Logger
) -> bool:
    collector = _get_collector(conn_id, task_logger)
    rows = collector.full_refresh(
        dataset_id=update_config.dataset_id,
        target_table=update_config.dataset_name,
        target_schema="raw_data",
        config=update_config,
    )
    task_logger.info(
        f"Full refresh: ingested {rows} rows into raw_data.{update_config.dataset_name}"
    )
    return True


@task
def run_incremental_update(
    conn_id: str, update_config: DatasetUpdateConfig, task_logger: Logger
) -> bool:
    collector = _get_collector(conn_id, task_logger)
    rows = collector.incremental_update(
        dataset_id=update_config.dataset_id,
        target_table=update_config.dataset_name,
        target_schema="raw_data",
        config=IncrementalConfig(
            incremental_column=":updated_at",
            conflict_key=update_config.entity_key or None,
        ),
        entity_key=update_config.entity_key or None,
    )
    task_logger.info(
        f"Incremental: ingested {rows} rows into raw_data.{update_config.dataset_name}"
    )
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def check_ingestion_log(
    conn_id: str, update_config: DatasetUpdateConfig, task_logger: Logger
) -> bool:
    pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    log_record = pg_engine.query(
        """
        select *
        from meta.ingest_log
        where dataset_id = %(dataset_id)s
        order by completed_at desc limit 1
        """,
        {"dataset_id": update_config.dataset_id},
    )
    task_logger.info(f"Ingestion log record: {log_record}")
    return True


@task_group
def update_socrata_table(
    update_config: DatasetUpdateConfig, conn_id: str, task_logger: Logger
) -> None:
    _update_mode = choose_update_mode(
        update_config=update_config, task_logger=task_logger
    )
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
