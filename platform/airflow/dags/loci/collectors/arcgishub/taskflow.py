# /loci_platform/platform/airflow/dags/loci/collectors/arcgishub/taskflow.py
from logging import Logger

from airflow.sdk import task, task_group
from airflow.sdk.bases.operator import chain
from loci.collectors.arcgishub.client import ArcGISHubClient
from loci.collectors.arcgishub.collector import ArcGISHubCollector
from loci.collectors.arcgishub.spec import ArcGISHubDatasetSpec
from loci.db.af_utils import get_postgres_engine
from loci.sources.update_configs import DatasetUpdateConfig
from loci.tasks.task_utils import check_ingestion_log, choose_update_mode
from loci.tracking.ingestion_tracker import IngestionTracker


def _get_collector(conn_id: str, base_url: str, task_logger: Logger) -> ArcGISHubCollector:
    pg_engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    tracker = IngestionTracker(engine=pg_engine)
    client = ArcGISHubClient(base_url)
    return ArcGISHubCollector(
        client=client,
        engine=pg_engine,
        tracker=tracker,
        logger=task_logger,
    )


@task
def run_full_update(
    conn_id: str,
    spec: ArcGISHubDatasetSpec,
    task_logger: Logger,
) -> dict:
    collector = _get_collector(conn_id, spec.base_url, task_logger)
    summary = collector.collect(spec, force=True)
    task_logger.info(
        "Collected %s: %d staged, %d merged",
        spec.name,
        summary["rows_staged"],
        summary["rows_merged"],
    )
    return summary


@task
def run_incremental_update(
    conn_id: str,
    spec: ArcGISHubDatasetSpec,
    task_logger: Logger,
) -> dict:
    collector = _get_collector(conn_id, spec.base_url, task_logger)
    summary = collector.collect(spec, force=False)
    task_logger.info(
        "Collected %s: %d staged, %d merged",
        spec.name,
        summary["rows_staged"],
        summary["rows_merged"],
    )
    return summary


@task_group
def update_arcgishub_table(
    update_config: DatasetUpdateConfig,
    conn_id: str,
    task_logger: Logger,
) -> None:
    _update_mode = choose_update_mode(update_config=update_config, task_logger=task_logger)
    _full_update = run_full_update(
        conn_id=conn_id,
        spec=update_config.spec,
        task_logger=task_logger,
    )
    _incremental_update = run_incremental_update(
        conn_id=conn_id,
        spec=update_config.spec,
        task_logger=task_logger,
    )
    _check_ingestion_log = check_ingestion_log(
        conn_id=conn_id, update_config=update_config, task_logger=task_logger
    )

    chain(_update_mode, _full_update, _check_ingestion_log)
    chain(_update_mode, _incremental_update, _check_ingestion_log)
