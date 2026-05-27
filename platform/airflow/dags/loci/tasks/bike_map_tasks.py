# loci_platform/platform/airflow/dags/loci/tasks/bike_map_tasks.py
"""
Shared task-graph builder for per-city bike map refresh DAGs.

Each city has its own DAG (e.g. refresh_bike_map_chicago) but the overall
shape is the same: build deps, build city-specific marts, export geojson,
deploy frontend, build routing graph, run tests, deploy graph, deploy
lambda. This module centralizes that shape; per-city DAGs just supply
the city name, dbt selectors, and the geocoding bbox.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

from airflow.sdk import Param, task
from airflow.sdk.bases.operator import chain
from loci.aws import get_boto_session
from loci.db.af_utils import get_postgres_engine
from loci.deploy import upload_file_to_s3
from loci.environments import VALID_ENVS, get_env
from loci.exports.bike_map_layers import LayerDisplayConfig, validate_layer_displays
from loci.exports.geojson_export import GeoJSONExportConfig
from loci.exports.graph_export import RoutingGraphExporter
from loci.tasks.deploy_tasks import deploy_bike_map, deploy_lambda, push_synthetic_fixture
from loci.tasks.export_tasks import export_bike_map_geojson
from loci.tasks.testing.deployment_smoke_test import (
    site_url_for,
    smoke_test_deployed_routing,
)
from loci.tasks.testing.route_tests import RouteTestCase, run_route_tests
from loci.tasks.transform_tasks import run_dbt

CONN_ID = "gis_dwh_db"
ENV_TEMPLATE = "{{ params.env }}"

task_logger = logging.getLogger("airflow.task")


@dataclass(frozen=True)
class CityBuildSpec:
    """Per-city configuration for the refresh DAG.

    city
        City identifier matching VALID_CITIES (e.g. 'chicago').
    bbox
        (south, west, north, east) in EPSG:4269 (NAD83 lon/lat) as the bounding box
        for geocoding and limiting to a bounding box around a city.
    weights_dbt_select
        dbt --select expression for the stress-weighted edges model that
        feeds the routing graph.
    geocode
        A bool indicating whether geocoding should be used for the city.
    pre_export_dbt_selects
        List of dbt --select expressions to run sequentially before the
        geojson export. Each runs as a separate dbt build invocation
        because they have ordering constraints (e.g. some need to run
        with --indirect-selection=cautious to skip cross-model tests).
    geojson_exports
        Per-mart-table export configs for the bike map. Each defines a
        mart table, geometry source, and properties to expose.
    layer_displays
        Per-layer frontend display config (paired with geojson_exports
        by name). Used by render_layers_config_json to produce the
        layers block for apps/bike-map/config/{env}/{city}.json.
    route_tests
        City-specific route quality test cases run after the routing
        graph is built. Empty list = no tests for this city (a warning
        is logged at run time).
    synthetic_check_fixture
        An (origin, destination) lat/lon pair used by the synthetic monitor
        as a known-good request payload. Each tuple is (lat, lon). None means
        no routing API check is run for this city.
    """

    city: str
    bbox: tuple[float, float, float, float]
    weights_dbt_select: str
    geocode: bool = False
    pre_export_dbt_selects: list[tuple[str, ...]] = field(default_factory=list)
    geojson_exports: list[GeoJSONExportConfig] = field(default_factory=list)
    layer_displays: list[LayerDisplayConfig] = field(default_factory=list)
    route_tests: list[RouteTestCase] = field(default_factory=list)
    synthetic_check_fixture: tuple[tuple[float, float], tuple[float, float]] | None = None

    def __post_init__(self):
        # Validate at DAG-parse time so typos in popup field formatters
        # or mismatched export names fail loudly when Airflow imports
        # the DAG, not silently in the browser.
        validate_layer_displays(self.layer_displays, self.geojson_exports)


@task
def install_dependencies(env: str) -> str:
    from airflow.sdk import get_current_context

    if get_current_context()["params"].get("skip_deps", False):
        task_logger.info("skip_deps=True; dbt deps assumed installed by parent DAG")
        return "skipped"

    target = get_env(env, _city_from_context()).dbt_target
    return run_dbt("deps", target=target)


def _city_from_context() -> str:
    """Helper for tasks that need city without it being a param.

    Reads from a context variable set by the per-city DAG factory.
    """
    from airflow.sdk import get_current_context

    return get_current_context()["params"]["city"]


@task
def run_dbt_select(env: str, select_args: tuple[str, ...]) -> str:
    """Generic dbt build with a list of --select args."""
    cfg = get_env(env, _city_from_context())
    return run_dbt("build", *select_args, target=cfg.dbt_target)


@task
def build_routing_graph(
    env: str, conn_id: str, graph_path: str, task_logger: logging.Logger
) -> str:
    """Build the stress-weighted routing graph for testing.

    Writes a gzip-compressed binary graph file to the given local path.
    The same file is later uploaded to S3 by `deploy_graph` and consumed by
    the Rust Lambda on cold start.
    """
    cfg = get_env(env, _city_from_context())
    engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    exporter = RoutingGraphExporter(engine, city=cfg.city, marts_schema=cfg.marts_schema)
    output_path = Path(graph_path)
    if output_path.exists():
        output_path.unlink()
        task_logger.info("Removed stale graph file at %s", output_path)
    exporter.export(output_path)
    task_logger.info("Built graph at %s", output_path)
    return str(output_path)


@task
def run_tests(
    graph_path: str,
    test_cases: list[RouteTestCase],
    task_logger: logging.Logger,
) -> list:
    """Runs tests that guard against regressions in routes between specified endpoints.

    Raises RuntimeError on any test failure so the task fails and the
    downstream deploy tasks are skipped. See loci.tasks.testing.route_tests
    for the assertion details.
    """
    results = run_route_tests(graph_path, test_cases, logger=task_logger)
    task_logger.info("Test results %s", results)
    return results


@task
def deploy_graph(env: str, graph_path: str, task_logger: logging.Logger) -> str:
    cfg = get_env(env, _city_from_context())
    uri = upload_file_to_s3(
        local_path=graph_path,
        bucket=cfg.routing_graph_bucket,
        key=cfg.routing_graph_key,
        logger=task_logger,
        s3_client=get_boto_session(cfg).client("s3"),
    )
    task_logger.info("Routing graph uploaded to %s", uri)
    return uri


@task
def smoke_test_deployment(
    env: str,
    bbox: tuple[float, float, float, float] | None,
    task_logger: logging.Logger,
) -> dict | None:
    if bbox is None:
        task_logger.info("No bbox configured; skipping smoke test")
        return None

    city = _city_from_context()
    cfg = get_env(env, city)
    return smoke_test_deployed_routing(
        site_url=site_url_for(city=city, env=env),
        bbox=bbox,
        env=env,
        cfg=cfg,
        logger=task_logger,
    )


def build_refresh_task_graph(spec: CityBuildSpec) -> None:
    """Build the standard refresh task graph for one city.

    The Rust routing Lambda and CLI are pre-built outside Airflow by
    `make build` in services/routing/, then mounted into the worker
    via the build-output compose volume. Deploy tasks just upload the
    pre-built artifacts; the route-tests task drives the pre-built
    CLI as a subprocess.
    """
    graph_path = f"/tmp/routing_graph_{{{{ params.env }}}}_{spec.city}.bin.gz"

    _deps = install_dependencies(env=ENV_TEMPLATE)

    if spec.geocode:
        from loci.tasks.transform_tasks import build_pre_geocode, geocode

        _pre_geocode = build_pre_geocode(env=ENV_TEMPLATE, city=spec.city)
        _geocode = geocode(
            conn_id=CONN_ID,
            task_logger=task_logger,
            restrict_region=spec.bbox,
        )
        chain(_deps, _pre_geocode, _geocode)
        _pre_export_root = _geocode
    else:
        _pre_export_root = _deps

    # Per-city dbt selects.
    pre_export_tasks = []
    for i, select_args in enumerate(spec.pre_export_dbt_selects):
        t = run_dbt_select.override(task_id=f"build_pre_export_{i}")(
            env=ENV_TEMPLATE, select_args=select_args
        )
        chain(_pre_export_root, t)
        pre_export_tasks.append(t)

    # Geojson export depends on all pre-export builds.
    _export_layers = export_bike_map_geojson(
        env=ENV_TEMPLATE,
        city=spec.city,
        conn_id=CONN_ID,
        exports=spec.geojson_exports,
        task_logger=task_logger,
    )
    chain(pre_export_tasks or [_pre_export_root], _export_layers)

    _deploy_map = deploy_bike_map(env=ENV_TEMPLATE, city=spec.city, task_logger=task_logger)
    chain(_export_layers, _deploy_map)

    # Routing graph: build → test → deploy. Linear chain — a test
    # failure short-circuits the deploys, preserving the safeguard
    # the original DAG provided.
    _build_weights = run_dbt_select.override(task_id="build_stress_weighted_edges")(
        env=ENV_TEMPLATE,
        select_args=("--select", spec.weights_dbt_select),
    )
    _build_graph = build_routing_graph(
        env=ENV_TEMPLATE,
        conn_id=CONN_ID,
        task_logger=task_logger,
        graph_path=graph_path,
    )
    _run_tests = run_tests(
        task_logger=task_logger,
        graph_path=graph_path,
        test_cases=spec.route_tests,
    )
    _deploy_graph = deploy_graph(
        env=ENV_TEMPLATE,
        graph_path=graph_path,
        task_logger=task_logger,
    )
    _deploy_lambda = deploy_lambda(
        env=ENV_TEMPLATE,
        city=spec.city,
        task_logger=task_logger,
    )
    chain(_deps, _build_weights, _build_graph, _run_tests, _deploy_graph, _deploy_lambda)

    _smoke_test = smoke_test_deployment(
        env=ENV_TEMPLATE,
        bbox=spec.bbox,
        task_logger=task_logger,
    )
    chain([_deploy_lambda, _deploy_map], _smoke_test)

    _push_fixture = push_synthetic_fixture(
        env=ENV_TEMPLATE,
        city=spec.city,
        fixture=spec.synthetic_check_fixture,
        task_logger=task_logger,
    )
    chain(_deploy_lambda, _push_fixture)


def standard_dag_params(city: str) -> dict:
    """Standard DAG params for a per-city refresh DAG."""
    return {
        "env": Param(
            "dev",
            type="string",
            enum=list(VALID_ENVS),
            title="Target environment",
            description="Which AWS account + dbt target to build and deploy to.",
        ),
        "city": Param(city, type="string", const=city),
        "skip_deps": Param(
            False,
            type="boolean",
            title="Skip dbt deps",
            description=(
                "Skip the install_dependencies task. Set automatically by the controller "
                "DAG (refresh_bike_map_all) so concurrent children don't race each other "
                "wiping and reinstalling dbt_packages/. Leave False for normal single-city runs."
            ),
        ),
    }
