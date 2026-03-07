from logging import Logger

from airflow.models.taskinstance import TaskInstance
from airflow.sdk import get_current_context, task
from airflow.task.trigger_rule import TriggerRule
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

    week_number = (logical_date.day - 1) // 7 + 1
    task_logger.info(f"Current week_number: {week_number}")
    task_logger.info(f"Current month:       {logical_date.month}")
    task_logger.info(f"Current day_of_week: {logical_date.day_of_week}")
    is_full_update = (
        logical_date.month in update_config.full_update_months
        and week_number == update_config.full_update_week_of_month
        and logical_date.day_of_week == update_config.full_update_day_of_week
    )

    if is_full_update:
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
        {"dataset_id": update_config.spec.dataset_id},
    )
    task_logger.info(f"Ingestion log record: {log_record}")
    return True
