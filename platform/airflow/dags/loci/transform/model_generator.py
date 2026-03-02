"""
Source-specific dbt model generators.

Each data source (Census, Socrata, TIGER, etc.) implements a subclass of
DbtPipelineBuilder that knows how to produce dbt models appropriate for that
source's data shape.

Usage:
    from dbt_model_generator import DbtModelGenerator
    from dbt_sources import CensusDbtPipelineBuilder
    from census_metadata import CensusMetadata
    from census_collector import CensusDatasetSpec

    gen = DbtModelGenerator("/path/to/dbt")
    cm = CensusMetadata(api_key="YOUR_KEY")
    census = CensusDbtPipelineBuilder(generator=gen, metadata=cm)

    spec = CensusDatasetSpec(
        name="commute_by_race",
        dataset="acs/acs5",
        vintages=[2022],
        groups=["B08105B"],
        geography_level="tract",
        target_table="commute_by_race_tract",
        target_schema="raw_data",
    )
    census.generate_loading_model(spec=spec, vintage=2022)
"""

from __future__ import annotations

import abc
from pathlib import Path

import yaml

# from dbt_model_generator import DbtModelGenerator
from loci.collectors.census.metadata import CensusMetadata
from loci.collectors.census.spec import CensusDatasetSpec
from loci.collectors.census.variable_mapper import CensusVariableMapper


class DbtPipelineBuilder(abc.ABC):
    """Base class for source-specific dbt model generators.

    Subclasses must implement generate_loading_model() with whatever
    signature is appropriate for that data source.

    Args:
        generator: A DbtModelGenerator instance pointed at the dbt project.
    """

    def __init__(self, generator: DbtModelGenerator):
        self.generator = generator

    @abc.abstractmethod
    def generate_loading_model(self, **kwargs) -> Path:
        """Generate a loading/staging model for this source."""
        ...


class DbtModelGenerator:
    """Generates dbt model SQL files and manages sources.yml entries.

    Args:
        dbt_project_dir: Path to the root of the dbt project
            (the directory containing dbt_project.yml).
    """

    def __init__(self, dbt_project_dir: str | Path):
        self.project_dir = Path(dbt_project_dir)
        self.models_dir = self.project_dir / "models"
        self.sources_path = self.models_dir / "sources.yml"

    def build_loading_sql(
        self,
        source_name: str,
        table_name: str,
        columns: list[str],
        is_scd2: bool = False,
        column_mapping: dict[str, str] | None = None,
        passthrough_columns: list[str] | None = None,
    ) -> str:
        """Build the SQL for a staging model without writing any files.

        Useful for previewing what a model will look like before generating it.

        Args:
            source_name: The source name in sources.yml (e.g. "raw_data").
            table_name: The table name within that source.
            columns: List of column names to alias in the select.
            is_scd2: If True, adds a CTE filtering to valid_to IS NULL.
            column_mapping: Optional dict of {original_col: desired_alias}.
                Columns not in this dict get the standardize_column_name macro.
            passthrough_columns: Optional list of columns to include in the
                select without any aliasing, placed before the aliased columns.

        Returns:
            The generated SQL string.
        """
        column_mapping = column_mapping or {}
        passthrough_columns = passthrough_columns or []
        return self._build_loading_sql(
            source_name,
            table_name,
            columns,
            is_scd2,
            column_mapping,
            passthrough_columns,
        )

    def print_loading_model(
        self,
        source_name: str,
        table_name: str,
        columns: list[str],
        is_scd2: bool = False,
        column_mapping: dict[str, str] | None = None,
        passthrough_columns: list[str] | None = None,
    ) -> None:
        """Print the SQL for a staging model without writing any files.

        Convenience method for notebook usage. Takes the same arguments
        as generate_loading_model().
        """
        sql = self.build_loading_sql(
            source_name,
            table_name,
            columns,
            is_scd2,
            column_mapping,
            passthrough_columns,
        )
        print(sql)

    def generate_loading_model(
        self,
        source_name: str,
        table_name: str,
        columns: list[str],
        is_scd2: bool = False,
        column_mapping: dict[str, str] | None = None,
        passthrough_columns: list[str] | None = None,
        overwrite: bool = False,
    ) -> Path:
        """Generate a staging model that selects from a source.

        For SCD2 sources, wraps the select in a CTE that filters to the
        current version (valid_to IS NULL). All columns are aliased — either
        via an explicit mapping or by applying the standardize_column_name
        macro.

        Args:
            source_name: The source name in sources.yml (e.g. "raw_data").
            table_name: The table name within that source.
            columns: List of column names to alias in the select.
            is_scd2: If True, adds a CTE filtering to valid_to IS NULL.
            column_mapping: Optional dict of {original_col: desired_alias}.
                Columns not in this dict get the standardize_column_name macro.
            passthrough_columns: Optional list of columns to include in the
                select without any aliasing, placed before the aliased columns.
            overwrite: If False (default), raises FileExistsError when the
                model file already exists.

        Returns:
            Path to the generated .sql file.
        """
        sql = self.build_loading_sql(
            source_name,
            table_name,
            columns,
            is_scd2,
            column_mapping,
            passthrough_columns,
        )

        model_dir = self.models_dir / "staging"
        model_dir.mkdir(parents=True, exist_ok=True)
        model_path = model_dir / f"stg__{table_name}.sql"

        if model_path.exists() and not overwrite:
            raise FileExistsError(
                f"{model_path} already exists. Pass overwrite=True to replace it."
            )

        model_path.write_text(sql)

        self._ensure_source_table(source_name, table_name)

        return model_path

    def _build_loading_sql(
        self,
        source_name: str,
        table_name: str,
        columns: list[str],
        is_scd2: bool,
        column_mapping: dict[str, str],
        passthrough_columns: list[str],
    ) -> str:
        source_ref = f"{{{{ source('{source_name}', '{table_name}') }}}}"

        select_lines = [f'"{col}"' for col in passthrough_columns]
        select_lines.extend(self._build_alias_lines(columns, column_mapping))
        select_body = ",\n        ".join(select_lines)

        cte_name = "current_records" if is_scd2 else "aliased"
        where_clause = "\n    where valid_to is null" if is_scd2 else ""

        return (
            f"with {cte_name} as (\n"
            f"    select\n"
            f"        {select_body}\n"
            f"    from {source_ref}{where_clause}\n"
            f")\n"
            f"\n"
            f"select * from {cte_name}\n"
        )

    def _build_alias_lines(self, columns: list[str], column_mapping: dict[str, str]) -> list[str]:
        lines = []
        for col in columns:
            if col in column_mapping:
                alias = column_mapping[col]
                lines.append(f'"{col}" as {alias}')
            else:
                lines.append(f'"{col}" as {{{{ standardize_column_name("{col}") }}}}')
        return lines

    def _ensure_source_table(self, source_name: str, table_name: str) -> None:
        """Add the table to sources.yml if it's not already listed."""
        sources_data = yaml.safe_load(self.sources_path.read_text())

        for source in sources_data.get("sources", []):
            if source["name"] == source_name:
                existing_tables = [t["name"] for t in source.get("tables", [])]
                if table_name not in existing_tables:
                    # source.setdefault("tables", []).append({"name": table_name})
                    tables = source.setdefault("tables", [])
                    tables.append({"name": table_name})
                    tables.sort(key=lambda t: t["name"])
                    self.sources_path.write_text(
                        yaml.dump(
                            sources_data,
                            default_flow_style=False,
                            sort_keys=False,
                            Dumper=IndentedDumper,
                        )
                    )
                return

        raise ValueError(
            f"Source '{source_name}' not found in {self.sources_path}. Add it manually first."
        )


class IndentedDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


class CensusDbtPipelineBuilder(DbtPipelineBuilder):
    """Generates dbt staging models for Census Bureau datasets.

    Handles the Census-specific concerns: resolving variable groups,
    compressing variable labels into column names, and passing through
    geography and vintage columns without aliasing.

    Args:
        generator: A DbtModelGenerator instance.
        metadata: A CensusMetadata instance for resolving variable labels.
        extra_abbreviations: Optional additional abbreviations for the
            variable mapper.
    """

    def __init__(
        self,
        generator: DbtModelGenerator,
        metadata: CensusMetadata,
        extra_abbreviations: dict[str, str] | None = None,
    ):
        super().__init__(generator)
        self._mapper = CensusVariableMapper(
            metadata=metadata,
            extra_abbreviations=extra_abbreviations,
        )

    def print_loading_model(
        self,
        spec: CensusDatasetSpec,
        vintage: int,
    ) -> Path:
        """Generate a staging model for a Census dataset.

        Resolves variable groups for the given vintage, compresses labels
        into short column names, and delegates to the DbtModelGenerator.

        Geography columns and vintage are passed through without aliasing.
        Variable columns are aliased via the compressed column map.

        Args:
            spec: The Census dataset specification.
            vintage: The vintage year to resolve variable metadata for.

        Returns:
            Path to the generated .sql file.
        """
        column_map = self._mapper.build_column_map(spec, vintage)
        self.generator.print_loading_model(
            source_name=spec.target_schema,
            table_name=spec.target_table,
            columns=list(column_map.keys()),
            is_scd2=True,
            column_mapping=column_map,
            passthrough_columns=spec.entity_key,
        )

    def generate_loading_model(
        self,
        spec: CensusDatasetSpec,
        vintage: int,
        overwrite: bool = False,
    ) -> Path:
        """Generate a staging model for a Census dataset.

        Resolves variable groups for the given vintage, compresses labels
        into short column names, and delegates to the DbtModelGenerator.

        Geography columns and vintage are passed through without aliasing.
        Variable columns are aliased via the compressed column map.

        Args:
            spec: The Census dataset specification.
            vintage: The vintage year to resolve variable metadata for.

        Returns:
            Path to the generated .sql file.
        """
        column_map = self._mapper.build_column_map(spec, vintage)

        return self.generator.generate_loading_model(
            source_name=spec.target_schema,
            table_name=spec.target_table,
            columns=list(column_map.keys()),
            is_scd2=True,
            column_mapping=column_map,
            passthrough_columns=spec.entity_key,
            overwrite=overwrite,
        )
