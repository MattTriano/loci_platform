import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, task_group
from airflow.sdk.bases.operator import chain
from loci.aws import get_boto_session
from loci.db.af_utils import get_postgres_engine
from loci.deploy import upload_file_to_s3
from loci.environments import get_env
from loci.exports.graph_export import RoutingGraphExporter
from loci.tasks.deploy_tasks import deploy_bike_map, deploy_lambda
from loci.tasks.export_tasks import export_bike_map_geojson
from loci.tasks.testing.route_tests import run_route_tests
from loci.tasks.transform_tasks import build_pre_geocode, geocode, run_dbt

task_logger = logging.getLogger("airflow.task")


CONN_ID = "gis_dwh_db"
GRAPH_PATH = "/tmp/routing_graph.pkl.gz"
DEFAULT_ENV = "dev"


@task
def install_dependencies(env: str) -> str:
    target = get_env(env).dbt_target
    return run_dbt("deps", target=target)


@task
def build_chicago_bike_theft_hotspots(env: str) -> str:
    target = get_env(env).dbt_target
    return run_dbt(
        "build",
        "--select",
        "geocoded_address_cache+,+chicago_bike_theft_hotspots",
        target=target,
    )


@task_group
def build_bike_marts_with_geocoding(
    env: str, conn_id: str, task_logger: logging.Logger, restrict_region: str | None
) -> None:
    _pre_build = build_pre_geocode(env=env)
    _geocode = geocode(conn_id=conn_id, task_logger=task_logger, restrict_region=restrict_region)
    _build_theft_hotspots = build_chicago_bike_theft_hotspots(env=env)

    chain(_pre_build, _geocode, _build_theft_hotspots)


@task
def build_chicago_bike_parking(env: str) -> str:
    target = get_env(env).dbt_target
    return run_dbt("build", "--select", "+chicago_bike_parking", target=target)


@task
def build_chicago_bike_crash_hotspots(env: str) -> str:
    target = get_env(env).dbt_target
    return run_dbt("build", "--select", "+bike_crash_hotspots", target=target)


@task
def build_bike_safety_weighted_edges(env: str) -> str:
    target = get_env(env).dbt_target
    return run_dbt("build", "--select", "+bike_safety_weighted_edges", target=target)


@task
def build_routing_graph(
    env: str, conn_id: str, graph_path: str, task_logger: logging.Logger
) -> str:
    """Build the safety-weighted routing graph for testing."""
    cfg = get_env(env)
    engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    exporter = RoutingGraphExporter(engine, marts_schema=cfg.marts_schema)
    output_path = Path(graph_path)
    if output_path.exists():
        output_path.unlink()
        task_logger.info("Removed stale graph file at %s", output_path)
    exporter.export(output_path)

    task_logger.info("Built graph to location %s", output_path)
    return str(output_path)


@task
def run_tests(graph_path: str, task_logger: logging.Logger) -> list:
    results = run_route_tests(graph_path, logger=task_logger)
    task_logger.info("Test results %s", results)
    return results


@task
def deploy_graph(env: str, graph_path: str, task_logger: logging.Logger) -> str:
    cfg = get_env(env)
    uri = upload_file_to_s3(
        local_path=graph_path,
        bucket=cfg.routing_graph_bucket,
        key=cfg.routing_graph_key,
        logger=task_logger,
        s3_client=get_boto_session(cfg).client("s3"),
    )
    task_logger.info("Routing graph uploaded to %s", uri)
    return uri


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt"],
)
def refresh_bike_map_app():
    env = DEFAULT_ENV

    _deps = install_dependencies(env=env)
    _build_geocoded_tables = build_bike_marts_with_geocoding(
        env=env,
        conn_id=CONN_ID,
        task_logger=task_logger,
        restrict_region="ST_MakeEnvelope(-87.94, 41.64, -87.52, 42.03, 4269)",
    )
    _build_parking_table = build_chicago_bike_parking(env=env)
    _build_crashes_table = build_chicago_bike_crash_hotspots(env=env)
    _export_layer_data = export_bike_map_geojson(
        env=env, conn_id=CONN_ID, task_logger=task_logger
    )
    _deploy_map = deploy_bike_map(env=env, task_logger=task_logger)
    chain(
        _deps,
        [_build_geocoded_tables, _build_parking_table, _build_crashes_table],
        _export_layer_data,
        _deploy_map,
    )

    _build_weights = build_bike_safety_weighted_edges(env=env)
    _build_graph = build_routing_graph(
        env=env, conn_id=CONN_ID, task_logger=task_logger, graph_path=GRAPH_PATH
    )
    _run_tests = run_tests(task_logger=task_logger, graph_path=GRAPH_PATH)
    _deploy_graph = deploy_graph(env=env, graph_path=GRAPH_PATH, task_logger=task_logger)

    _deploy_lambda = deploy_lambda(env=env, task_logger=task_logger)

    chain(
        _deps,
        _build_weights,
        _build_graph,
        _run_tests,
        _deploy_graph,
        _deploy_lambda,
    )


refresh_bike_map_app()
