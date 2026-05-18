# loci_platform/platform/airflow/dags/loci/transform/generators/pipeline_builder.py
"""
Base class for city pipeline builders.

A "city pipeline" is a set of dbt model files (staging + marts) and test ymls
that scaffold a domain-specific data product for a new city. Each pipeline
delegates to parameterized dbt macros; this builder writes the thin per-city
wrapper files.

Subclasses declare what to generate via class attributes (templates, test
specs, source tables to register) and expose a concrete generate() method
with a per-pipeline argument signature.

The base owns:
  - Directory conventions (models/staging/<domain>/<city>/, models/marts/<domain>/<city>/)
  - Test yml filenames (_stg_<city>_<domain>.yml, _marts_<city>_<domain>.yml)
  - File writing (skip if exists, optional overwrite)
  - Yml merging (preserve hand-edits, append missing model entries)
  - Source-table registration (in _<source>_sources.yml)
"""

from __future__ import annotations

import abc
from pathlib import Path

import yaml
from loci.transform.model_generator import IndentedDumper


class CityPipelineBuilder(abc.ABC):
    """Base class for city pipeline builders.

    Subclasses set the class attributes below to declare what their pipeline
    generates, then implement generate() with a pipeline-specific signature
    and delegate to _generate_pipeline().

    Class attributes:
        DOMAIN: subject-area folder name, e.g. "cycling".
        SOURCE_NAME: dbt source name the pipeline reads from, e.g. "osm".
        SOURCE_TABLES: list of {city}-templated source table names to ensure
            are registered in _<SOURCE_NAME>_sources.yml. Empty list if the
            pipeline reads only from refs.
        STAGING_MODELS, MARTS_MODELS: dicts of {name_template: body_template}
            for the .sql wrappers. Templates are str.format()'d with `city`
            and any per-call fmt_kwargs.
        STAGING_TESTS, MARTS_TESTS: lists of model entry dicts mirroring the
            structure of the dbt schema yml. {city} placeholders are
            recursively substituted at write time.

    Args:
        dbt_project_dir: Path to the root of the dbt project (the directory
            containing dbt_project.yml).
    """

    DOMAIN: str = ""
    SOURCE_NAME: str = ""
    SOURCE_TABLES: list[str] = []

    STAGING_MODELS: dict[str, str] = {}
    MARTS_MODELS: dict[str, str] = {}
    STAGING_TESTS: list[dict] = []
    MARTS_TESTS: list[dict] = []

    def __init__(self, dbt_project_dir: str | Path):
        self.project_dir = Path(dbt_project_dir)
        self.models_dir = self.project_dir / "models"

    @abc.abstractmethod
    def generate(self, city: str, **kwargs) -> list[Path]:
        """Generate the pipeline for a city. Each subclass declares its own
        concrete signature (e.g. include_crashes: bool, time_zone: str) and
        delegates to _generate_pipeline()."""
        ...

    # ------------------------------------------------------------------
    # Core orchestration
    # ------------------------------------------------------------------

    def _generate_pipeline(
        self,
        city: str,
        overwrite: bool = False,
        fmt_kwargs: dict | None = None,
        staging_models: dict[str, str] | None = None,
        marts_models: dict[str, str] | None = None,
    ) -> list[Path]:
        """Write all pipeline files for a city.

        Existing .sql files are skipped (unless overwrite=True). Yml files
        are merged into — model entries already present are left untouched.

        Args:
            city: city name.
            overwrite: when True, overwrite existing .sql files.
            fmt_kwargs: extra keyword args for str.format() on each SQL template
                (in addition to `city`, which is always passed).
            staging_models: overrides cls.STAGING_MODELS. Use when the
                subclass needs to add/remove models conditionally.
            marts_models: overrides cls.MARTS_MODELS.

        Returns:
            Paths of files written or modified.
        """
        if not self.DOMAIN:
            raise ValueError(f"{type(self).__name__} must set DOMAIN.")

        fmt_kwargs = fmt_kwargs or {}
        staging_models = staging_models if staging_models is not None else self.STAGING_MODELS
        marts_models = marts_models if marts_models is not None else self.MARTS_MODELS

        staging_dir = self.models_dir / "staging" / self.DOMAIN / city
        marts_dir = self.models_dir / "marts" / self.DOMAIN / city

        written: list[Path] = []

        # SQL files
        written.extend(
            self._write_sql_files(staging_dir, staging_models, city, overwrite, fmt_kwargs)
        )
        written.extend(self._write_sql_files(marts_dir, marts_models, city, overwrite, fmt_kwargs))

        # Test ymls
        staging_yml = staging_dir / f"_stg_{city}_{self.DOMAIN}.yml"
        marts_yml = marts_dir / f"_marts_{city}_{self.DOMAIN}.yml"

        if self.STAGING_TESTS:
            if path := self._ensure_yml_tests(staging_yml, self.STAGING_TESTS, city):
                written.append(path)
        if self.MARTS_TESTS:
            if path := self._ensure_yml_tests(marts_yml, self.MARTS_TESTS, city):
                written.append(path)

        # Source registration
        if self.SOURCE_NAME and self.SOURCE_TABLES:
            if path := self._ensure_source_tables(city):
                written.append(path)

        return written

    # ------------------------------------------------------------------
    # SQL file writing
    # ------------------------------------------------------------------

    def _write_sql_files(
        self,
        target_dir: Path,
        templates: dict[str, str],
        city: str,
        overwrite: bool,
        fmt_kwargs: dict,
    ) -> list[Path]:
        """Write .sql files from templates into target_dir. Skips files that
        already exist unless overwrite=True."""
        if not templates:
            return []
        target_dir.mkdir(parents=True, exist_ok=True)

        written: list[Path] = []
        for name_template, body_template in templates.items():
            name = name_template.format(city=city)
            path = target_dir / f"{name}.sql"
            if path.exists() and not overwrite:
                continue
            body = body_template.format(city=city, **fmt_kwargs)
            path.write_text(body)
            written.append(path)
        return written

    # ------------------------------------------------------------------
    # YML test merging
    # ------------------------------------------------------------------

    def _ensure_yml_tests(
        self,
        yml_path: Path,
        test_specs: list[dict],
        city: str,
    ) -> Path | None:
        """Ensure a per-pipeline test yml has model entries for each spec.

        If the yml doesn't exist, it's created with all entries. If it does,
        only entries that aren't already present are added — existing ones
        are left untouched.

        Returns the yml path if changed, or None if no changes were needed.
        """
        yml_path.parent.mkdir(parents=True, exist_ok=True)

        rendered_specs = [_render_city_placeholders(s, city) for s in test_specs]

        if yml_path.exists():
            data = yaml.safe_load(yml_path.read_text()) or {}
        else:
            data = {"version": 2, "models": []}

        data.setdefault("version", 2)
        models = data.setdefault("models", [])
        existing_names = {m["name"] for m in models if isinstance(m, dict) and "name" in m}

        added = False
        for spec in rendered_specs:
            if spec["name"] not in existing_names:
                models.append(spec)
                added = True

        if not added and yml_path.exists():
            return None

        yml_path.write_text(
            yaml.dump(
                data,
                default_flow_style=False,
                sort_keys=False,
                Dumper=IndentedDumper,
            )
        )
        return yml_path

    # ------------------------------------------------------------------
    # Source registration
    # ------------------------------------------------------------------

    def _ensure_source_tables(self, city: str) -> Path | None:
        """Add the pipeline's source tables to _<SOURCE_NAME>_sources.yml.

        The sources file is expected to already exist with a top-level source
        entry matching SOURCE_NAME. Tables in SOURCE_TABLES are str.format()'d
        with city, added if missing, and the final list is sorted.

        Returns the path if changed, or None if all tables were already present.
        Raises FileNotFoundError if the sources file doesn't exist, or
        ValueError if the source entry isn't in it.
        """
        sources_path = self.models_dir / f"_{self.SOURCE_NAME}_sources.yml"
        if not sources_path.exists():
            raise FileNotFoundError(
                f"{sources_path} not found. Create it before generating pipelines."
            )

        data = yaml.safe_load(sources_path.read_text())

        source_entry = None
        for source in data.get("sources", []):
            if source["name"] == self.SOURCE_NAME:
                source_entry = source
                break
        if source_entry is None:
            raise ValueError(
                f"Source '{self.SOURCE_NAME}' not found in {sources_path}. Add it manually first."
            )

        required = [t.format(city=city) for t in self.SOURCE_TABLES]
        tables = source_entry.setdefault("tables", [])
        existing = {t["name"] for t in tables if isinstance(t, dict) and "name" in t}

        added = False
        for name in required:
            if name not in existing:
                tables.append({"name": name})
                added = True

        if not added:
            return None

        tables.sort(key=lambda t: t["name"])
        sources_path.write_text(
            yaml.dump(
                data,
                default_flow_style=False,
                sort_keys=False,
                Dumper=IndentedDumper,
            )
        )
        return sources_path


class BikeStressPipelineBuilder(CityPipelineBuilder):
    """Generates the dbt model files and test ymls for a city's bike-stress-weighted
    segments pipeline.

    The pipeline is a chain of staging models that lift OSM edges/nodes into typed
    bike-network segments, then a chain of marts models that score those segments
    with stress costs (physical + intersection + optional crash). All steps
    delegate to parameterized dbt macros.

    Usage:
        builder = BikeStressPipelineBuilder("/path/to/dbt")
        builder.generate(
            city="detroit",
            include_crashes=False,
            include_all_sidewalks=False,
        )
    """

    DOMAIN = "cycling"
    SOURCE_NAME = "osm"
    SOURCE_TABLES = [
        "{city}_osm_bike_network_edges",
        "{city}_osm_bike_network_nodes",
    ]

    STAGING_MODELS: dict[str, str] = {
        "stg_{city}_bike_ways": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_stg_city_bike_ways_model('{city}', include_all_sidewalks={include_all_sidewalks}) }}}}
""",
        "stg_{city}_way_nodes": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_stg_city_way_nodes_model('{city}') }}}}
""",
        "stg_{city}_intersection_nodes": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_stg_city_intersection_nodes_model('{city}') }}}}
""",
        "stg_{city}_way_segments": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_stg_city_way_segments_model('{city}') }}}}
""",
        "stg_{city}_osm_bike_infra": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_stg_city_osm_bike_infra_model('{city}', include_all_sidewalks={include_all_sidewalks}) }}}}
""",
        "stg_{city}_bike_segments": """\
{{{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{{{ this }}}} (way_id, start_position, end_position)",
        "CREATE INDEX ON {{{{ this }}}} (start_node_id)",
        "CREATE INDEX ON {{{{ this }}}} (end_node_id)",
        "CREATE INDEX ON {{{{ this }}}} USING GIST (geom)",
        "ANALYZE {{{{ this }}}}"
    ]
) }}}}

{{{{ generate_stg_city_bike_segments_model('{city}') }}}}
""",
    }

    MARTS_MODELS_ALWAYS: dict[str, str] = {
        "{city}_segment_costs": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_city_segment_costs_model('{city}') }}}}
""",
        "{city}_intersection_costs": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_stg_city_intersection_costs_model('{city}') }}}}
""",
        "{city}_bike_stress_weighted_segments": """\
{{{{ config(
    materialized='table',
    pre_hook=["SET work_mem = '256MB'"],
    post_hook=[
        "CREATE INDEX ON {{{{ this }}}} (segment_id)",
        "CREATE INDEX ON {{{{ this }}}} (start_node_id)",
        "CREATE INDEX ON {{{{ this }}}} (end_node_id)",
        "CREATE INDEX ON {{{{ this }}}} USING GIST (geom)",
        "RESET work_mem"
    ]
) }}}}

{{{{ generate_city_bike_stress_weighted_segments_model('{city}', include_crashes={include_crashes}) }}}}
""",
    }

    MARTS_MODELS_CRASHES: dict[str, str] = {
        "{city}_segment_crash_costs": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_city_segment_crash_costs_model('{city}') }}}}
""",
    }

    STAGING_TESTS: list[dict] = [
        {
            "name": "stg_{city}_bike_ways",
            "columns": [
                {"name": "osm_id", "data_tests": ["not_null", "unique"]},
                {"name": "geom", "data_tests": ["not_null"]},
                {"name": "node_ids", "data_tests": ["not_null"]},
                {
                    "name": "node_count",
                    "data_tests": [
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 2}}},
                    ],
                },
                {"name": "highway", "data_tests": ["dbt_utils.not_empty_string"]},
            ],
        },
        {
            "name": "stg_{city}_way_nodes",
            "data_tests": [
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "arguments": {
                            "combination_of_columns": ["way_id", "position"],
                        },
                    },
                },
            ],
            "columns": [
                {
                    "name": "way_id",
                    "data_tests": [
                        "not_null",
                        {
                            "relationships": {
                                "arguments": {
                                    "to": "ref('stg_{city}_bike_ways')",
                                    "field": "osm_id",
                                },
                            },
                        },
                    ],
                },
                {
                    "name": "position",
                    "data_tests": [
                        "not_null",
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 1}}},
                    ],
                },
                {"name": "node_id", "data_tests": ["not_null"]},
            ],
        },
        {
            "name": "stg_{city}_intersection_nodes",
            "data_tests": [
                {
                    "intersection_node_count_in_range": {
                        "arguments": {
                            "way_nodes_model": "ref('stg_{city}_way_nodes')",
                        },
                    },
                },
            ],
            "columns": [
                {"name": "node_id", "data_tests": ["not_null", "unique"]},
            ],
        },
        {
            "name": "stg_{city}_way_segments",
            "data_tests": [
                {
                    "segment_endpoints_match_node_ids": {
                        "arguments": {"ways_model": "ref('stg_{city}_bike_ways')"},
                    },
                },
                {
                    "segments_cover_full_ways": {
                        "arguments": {
                            "way_nodes_model": "ref('stg_{city}_way_nodes')",
                            "intersection_nodes_model": "ref('stg_{city}_intersection_nodes')",
                        },
                    },
                },
                {
                    "segment_geom_endpoints_match_node_positions": {
                        "arguments": {"ways_model": "ref('stg_{city}_bike_ways')"},
                    },
                },
            ],
        },
    ]

    MARTS_TESTS: list[dict] = [
        {
            "name": "{city}_segment_costs",
            "description": (
                "Per-segment intrinsic stress costs from OSM physical attributes "
                "(highway class, infrastructure, surface, lighting, tunnel)."
            ),
            "data_tests": [
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "arguments": {"combination_of_columns": ["segment_id"]},
                    },
                },
                {
                    "dbt_utils.expression_is_true": {
                        "arguments": {"expression": "physical_cost >= 0"},
                        "config": {"name": "{city}_segment_costs_physical_cost_non_negative"},
                    },
                },
                {
                    "dbt_utils.equal_rowcount": {
                        "arguments": {"compare_model": "ref('stg_{city}_bike_segments')"},
                    },
                },
            ],
            "columns": [
                {"name": "segment_id", "data_tests": ["not_null", "unique"]},
                {"name": "way_id", "data_tests": ["not_null"]},
                {"name": "start_node_id", "data_tests": ["not_null"]},
                {"name": "end_node_id", "data_tests": ["not_null"]},
                {
                    "name": "length_m",
                    "data_tests": [
                        "not_null",
                        {
                            "dbt_expectations.expect_column_values_to_be_between": {
                                "arguments": {"min_value": 0, "strictly": True},
                            },
                        },
                    ],
                },
                {"name": "geom", "data_tests": ["not_null"]},
                {
                    "name": "speed_factor",
                    "data_tests": [
                        "not_null",
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 1.0}}},
                    ],
                },
                {
                    "name": "road_type_factor",
                    "data_tests": [
                        "not_null",
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 1.0}}},
                    ],
                },
                {
                    "name": "infrastructure_factor",
                    "data_tests": [
                        "not_null",
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 1.0}}},
                    ],
                },
                {
                    "name": "tunnel_factor",
                    "data_tests": [
                        "not_null",
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 1.0}}},
                    ],
                },
                {
                    "name": "surface_factor",
                    "data_tests": [
                        "not_null",
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 1.0}}},
                    ],
                },
                {
                    "name": "lighting_factor",
                    "data_tests": [
                        "not_null",
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 1.0}}},
                    ],
                },
                {
                    "name": "physical_cost",
                    "data_tests": [
                        "not_null",
                        {"dbt_utils.accepted_range": {"arguments": {"min_value": 0}}},
                    ],
                },
                {
                    "name": "infra_category",
                    "data_tests": [
                        {
                            "accepted_values": {
                                "arguments": {
                                    "values": ["separated", "on_road", "shared", "path"],
                                },
                                "config": {"where": "infra_category is not null"},
                            },
                        },
                    ],
                },
            ],
        },
        {
            "name": "{city}_intersection_costs",
            "description": (
                "Logical bike-network intersections with stress penalties. A row "
                "exists for each node where >= 2 distinct OSM ways meet AND >= 1 "
                "of those ways carries cars. Joined into "
                "{city}_bike_stress_weighted_segments on end_node_id."
            ),
            "columns": [
                {
                    "name": "osmid",
                    "description": "OSM node id (unique).",
                    "data_tests": [
                        "not_null",
                        "unique",
                        {
                            "dbt_utils.relationships_where": {
                                "arguments": {
                                    "to": "source('osm', '{city}_osm_bike_network_nodes')",
                                    "field": "osm_id",
                                    "from_condition": "traffic_control is not null",
                                    "to_condition": "osm_type = 'node' and valid_to is null",
                                },
                                "config": {"severity": "warn"},
                            },
                        },
                    ],
                },
                {
                    "name": "traffic_control",
                    "description": (
                        "Raw OSM 'highway' tag value on the node, or NULL for "
                        "uncontrolled intersections."
                    ),
                    "data_tests": [
                        {
                            "accepted_values": {
                                "arguments": {
                                    "values": [
                                        "traffic_signals",
                                        "stop",
                                        "crossing",
                                        "mini_roundabout",
                                        "give_way",
                                        "turning_circle",
                                        "turning_loop",
                                    ],
                                    "quote": True,
                                },
                            },
                        },
                    ],
                },
                {
                    "name": "max_road_class",
                    "description": "Worst road class among car-carrying ways at this node.",
                    "data_tests": [
                        "not_null",
                        {
                            "accepted_values": {
                                "arguments": {
                                    "values": ["primary", "secondary", "tertiary", "minor"],
                                },
                            },
                        },
                    ],
                },
                {
                    "name": "max_road_rank",
                    "description": "Numeric form of max_road_class (1-4).",
                    "data_tests": [
                        "not_null",
                        {
                            "dbt_utils.accepted_range": {
                                "arguments": {"min_value": 1, "max_value": 4},
                            },
                        },
                    ],
                },
                {
                    "name": "intersection_cost",
                    "description": (
                        "Stress cost added to a route for traversing through this intersection."
                    ),
                    "data_tests": ["not_null"],
                },
            ],
        },
        {
            "name": "{city}_bike_stress_weighted_segments",
            "description": (
                "Stress-weighted directed segments for bike routing. One row per "
                "directed edge (u, v, key). stress_cost is the primary routing "
                "weight, combining length, speed, infrastructure, surface, "
                "lighting, crash history, and traffic control penalties."
            ),
            "data_tests": [
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "arguments": {
                            "combination_of_columns": [
                                "start_node_id",
                                "end_node_id",
                                "way_id",
                            ],
                        },
                    },
                },
                {
                    "dbt_utils.expression_is_true": {
                        "arguments": {"expression": "stress_cost >= 0"},
                        "config": {"name": "{city}_stress_cost_non_negative"},
                    },
                },
                {
                    "dbt_utils.expression_is_true": {
                        "arguments": {"expression": "crash_cost >= 0"},
                        "config": {"name": "{city}_crash_cost_non_negative"},
                    },
                },
                {
                    "dbt_utils.equal_rowcount": {
                        "arguments": {"compare_model": "ref('stg_{city}_bike_segments')"},
                    },
                },
            ],
            "columns": [
                {"name": "start_node_id", "data_tests": ["not_null"]},
                {"name": "end_node_id", "data_tests": ["not_null"]},
                {"name": "way_id", "data_tests": ["not_null"]},
                {
                    "name": "length_m",
                    "data_tests": [
                        "not_null",
                        {
                            "dbt_expectations.expect_column_values_to_be_between": {
                                "arguments": {"min_value": 0, "strictly": True},
                            },
                        },
                    ],
                },
                {"name": "stress_cost", "data_tests": ["not_null"]},
                {"name": "speed_factor", "data_tests": ["not_null"]},
                {"name": "infrastructure_factor", "data_tests": ["not_null"]},
                {"name": "surface_factor", "data_tests": ["not_null"]},
                {"name": "lighting_factor", "data_tests": ["not_null"]},
                {"name": "crash_cost", "data_tests": ["not_null"]},
                {"name": "intersection_cost", "data_tests": ["not_null"]},
                {"name": "geom", "data_tests": ["not_null"]},
            ],
        },
    ]
    MARTS_MODELS = MARTS_MODELS_ALWAYS

    def generate(
        self,
        city: str,
        include_crashes: bool,
        include_all_sidewalks: bool,
        overwrite: bool = False,
    ) -> list[Path]:
        """Generate all dbt files for a city's bike-stress pipeline.

        Args:
            city: city name.
            include_crashes: when True, also writes <city>_segment_crash_costs
                and tells the stress-weighted-segments macro to join crash costs.
            include_all_sidewalks: passed through to the bike_ways and
                osm_bike_infra macros.
            overwrite: when True, overwrite existing .sql files.
        """
        marts_models = dict(self.MARTS_MODELS_ALWAYS)
        if include_crashes:
            marts_models.update(self.MARTS_MODELS_CRASHES)

        return self._generate_pipeline(
            city=city,
            overwrite=overwrite,
            fmt_kwargs={
                "include_crashes": jinja_bool(include_crashes),
                "include_all_sidewalks": jinja_bool(include_all_sidewalks),
            },
            marts_models=marts_models,
        )


class BikeIndexTheftsPipelineBuilder(CityPipelineBuilder):
    """Generates the dbt model files for a city's BikeIndex bike-thefts pipeline.

    Pipeline shape: one staging model that lifts the raw BikeIndex source into a
    typed table, then one marts model that produces the city-facing theft dataset.
    The marts macro takes a time_zone argument so timestamps can be presented in
    the city's local time.

    Usage:
        builder = BikeIndexTheftsPipelineBuilder("/path/to/dbt")
        builder.generate(city="detroit", time_zone="America/Detroit")
    """

    DOMAIN = "cycling"
    SOURCE_NAME = "bikeindex"
    SOURCE_TABLES = ["{city}_bikeindex_bike_thefts"]

    STAGING_MODELS: dict[str, str] = {
        "stg_{city}_bikeindex_bike_thefts": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_stg_city_bikeindex_bike_thefts_model('{city}') }}}}
""",
    }

    MARTS_MODELS: dict[str, str] = {
        "{city}_bikeindex_bike_thefts": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_city_bikeindex_bike_thefts_model('{city}', time_zone = '{time_zone}') }}}}
""",
    }
    STAGING_TESTS: list[dict] = []
    MARTS_TESTS: list[dict] = []

    def generate(
        self,
        city: str,
        time_zone: str,
        overwrite: bool = False,
    ) -> list[Path]:
        """Generate all dbt files for a city's BikeIndex thefts pipeline.

        Args:
            city: city name.
            time_zone: IANA timezone string (e.g. "America/Detroit") passed
                to the marts macro for local-time presentation.
            overwrite: when True, overwrite existing .sql files.
        """
        return self._generate_pipeline(
            city=city,
            overwrite=overwrite,
            fmt_kwargs={"time_zone": time_zone},
        )


class OsmBikeParkingPipelineBuilder(CityPipelineBuilder):
    """Generates the dbt model files for a city's OSM bike-parking pipeline.

    Pipeline shape: one staging model that lifts the raw OSM bike-parking source
    into a typed table, then one marts model that produces the city-facing
    bike-parking dataset.

    Usage:
        builder = OsmBikeParkingPipelineBuilder("/path/to/dbt")
        builder.generate(city="detroit")
    """

    DOMAIN = "cycling"
    SOURCE_NAME = "osm"
    SOURCE_TABLES = ["{city}_osm_bike_parking"]

    STAGING_MODELS: dict[str, str] = {
        "stg_{city}_osm_bike_parking": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_stg_city_osm_bike_parking_model('{city}') }}}}
""",
    }
    MARTS_MODELS: dict[str, str] = {
        "{city}_osm_bike_parking": """\
{{{{ config(materialized='table') }}}}

{{{{ generate_city_osm_bike_parking_model('{city}') }}}}
""",
    }

    STAGING_TESTS: list[dict] = []
    MARTS_TESTS: list[dict] = []

    def generate(self, city: str, overwrite: bool = False) -> list[Path]:
        """Generate all dbt files for a city's OSM bike-parking pipeline.

        Args:
            city: city name.
            overwrite: when True, overwrite existing .sql files.
        """
        return self._generate_pipeline(city=city, overwrite=overwrite)


# =====================================================================
# Helpers
# =====================================================================


def _render_city_placeholders(obj, city: str):
    """Recursively substitute {city} into all string values of a test spec."""
    if isinstance(obj, str):
        return obj.format(city=city)
    if isinstance(obj, dict):
        return {k: _render_city_placeholders(v, city) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_render_city_placeholders(v, city) for v in obj]
    return obj


def jinja_bool(value: bool) -> str:
    """Render a Python bool as a lowercase Jinja literal ('true'/'false')."""
    return "true" if value else "false"
