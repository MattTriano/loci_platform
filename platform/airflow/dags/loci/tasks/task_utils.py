from logging import Logger

import pendulum
from airflow.models.taskinstance import TaskInstance
from airflow.sdk import get_current_context, task
from airflow.task.trigger_rule import TriggerRule
from croniter import croniter

from loci.db.af_utils import get_postgres_engine
from loci.sources.update_configs import DatasetUpdateConfig


def get_task_group_id_prefix(task_instance: TaskInstance) -> str:
    task_id_parts = task_instance.task_id.split(".")
    if len(task_id_parts) > 1:
        return ".".join(task_id_parts[:-1]) + "."
    else:
        return ""


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
