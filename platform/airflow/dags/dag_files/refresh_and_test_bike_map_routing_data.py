import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain
from loci.db.af_utils import get_postgres_engine
from loci.exports.graph_export import RoutingGraphExporter
from loci.tasks.testing.route_tests import run_route_tests
from loci.tasks.transform_tasks import run_dbt

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"
GRAPH_PATH = "/tmp/routing_graph.pkl.gz"


@task
def build_bike_safety_weighted_edges() -> str:
    return run_dbt("build", "--select", "+bike_safety_weighted_edges")


@task
def build_routing_graph(conn_id: str, task_logger: logging.Logger, graph_path: str) -> str:
    """Build the safety-weighted routing graph for testing."""
    engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    exporter = RoutingGraphExporter(engine)
    output_path = Path(graph_path)
    if output_path.exists():
        output_path.unlink()
        task_logger.info("Removed stale graph file at %s", output_path)
    exporter.export(output_path)

    task_logger.info("Built graph to location %s", output_path)
    return str(output_path)


@task
def run_tests(task_logger: logging.Logger, graph_path) -> list:
    results = run_route_tests(graph_path, logger=task_logger)
    task_logger.info("Test results %s", results)
    return results


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * 0",
    catchup=False,
    tags=["dbt"],
)
def refresh_and_test_bike_map_routing_data():
    _build_weights = build_bike_safety_weighted_edges()
    _export_graph = build_routing_graph(
        conn_id=CONN_ID, task_logger=task_logger, graph_path=GRAPH_PATH
    )
    _run_tests = run_tests(task_logger=task_logger, graph_path=GRAPH_PATH)

    chain(
        _build_weights,
        _export_graph,
        _run_tests,
    )


refresh_and_test_bike_map_routing_data()
