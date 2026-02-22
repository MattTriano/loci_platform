"""
Census data collection and ingestion pipeline.

Classes:
    CensusDatasetSpec  — defines what census data to collect
    CensusClient       — makes Census Bureau API calls
    CensusCollector    — orchestrates collection and ingestion to Postgres

Usage:
    from census_collector import CensusDatasetSpec, CensusClient, CensusCollector

    spec = CensusDatasetSpec(
        name="occupation_by_sex",
        dataset="acs/acs5",
        vintages=[2019, 2020, 2021, 2022],
        groups=["B24010"],
        variables=["B01001_001E"],
        geography_level="tract",
        target_table="occupation_by_sex_tract",
        target_schema="raw_data",
    )

    client = CensusClient(api_key="YOUR_KEY")
    collector = CensusCollector(client=client, engine=engine)
    collector.collect(spec)
"""

from __future__ import annotations

import logging

from collectors.census.client import CensusClient
from collectors.census.spec import GEOGRAPHY_CONFIG, CensusDatasetSpec

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Collector
# ------------------------------------------------------------------ #

METADATA_COLUMNS = {
    "ingested_at",
    "record_hash",
    "valid_from",
    "valid_to",
}


class CensusCollector:
    """
    Orchestrates collecting and ingesting census data defined by a
    CensusDatasetSpec.

    Iterates over vintages × states, checks the target table for
    already-ingested data, fetches from the Census API, and ingests
    via StagedIngest.

    Parameters
    ----------
    client : CensusClient
    engine : PostgresEngine
    tracker : IngestionTracker, optional
        If provided, logs each state+vintage run.
    """

    def __init__(self, client, engine, tracker=None):
        self.client = client
        self.engine = engine
        self.tracker = tracker
        self.logger = logging.getLogger("census_collector")

    def collect(self, spec: CensusDatasetSpec, force: bool = False) -> dict:
        """
        Collect and ingest all data defined by a CensusDatasetSpec.

        Parameters
        ----------
        spec : CensusDatasetSpec
        force : bool
            If True, skip idempotency checks and re-ingest everything.

        Returns a summary dict with counts.
        """
        summary = {
            "spec_name": spec.name,
            "vintages_processed": 0,
            "states_processed": 0,
            "states_skipped": 0,
            "total_rows_staged": 0,
            "total_rows_merged": 0,
            "errors": [],
        }

        for vintage in spec.vintages:
            # Resolve the full variable list once per vintage (groups can
            # theoretically differ across vintages)
            variables = self.client.resolve_all_variables(spec, vintage)
            self.logger.info(
                "Spec %r vintage %d: %d total variables resolved",
                spec.name,
                vintage,
                len(variables),
            )

            for state_fips in spec.states:
                if not force and self._already_ingested(spec, vintage, state_fips):
                    self.logger.info(
                        "Skipping %s vintage=%d state=%s (already ingested)",
                        spec.name,
                        vintage,
                        state_fips,
                    )
                    summary["states_skipped"] += 1
                    continue

                try:
                    staged, merged = self._collect_state_vintage(
                        spec,
                        vintage,
                        state_fips,
                        variables,
                    )
                    summary["states_processed"] += 1
                    summary["total_rows_staged"] += staged
                    summary["total_rows_merged"] += merged
                except Exception as e:
                    self.logger.error(
                        "Failed: %s vintage=%d state=%s: %s",
                        spec.name,
                        vintage,
                        state_fips,
                        e,
                    )
                    summary["errors"].append(
                        {
                            "vintage": vintage,
                            "state_fips": state_fips,
                            "error": str(e),
                        }
                    )

            summary["vintages_processed"] += 1

        self.logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    # def _collect_state_vintage(
    #     self,
    #     spec: CensusDatasetSpec,
    #     vintage: int,
    #     state_fips: str,
    #     variables: list[str],
    # ) -> tuple[int, int]:
    #     """
    #     Fetch and ingest data for one state + vintage combination.

    #     Returns (rows_staged, rows_merged).
    #     """
    #     self.logger.info(
    #         "Collecting %s vintage=%d state=%s", spec.name, vintage, state_fips,
    #     )

    #     # Fetch from Census API
    #     rows = self.client.fetch_variables(
    #         dataset=spec.dataset,
    #         vintage=vintage,
    #         variables=variables,
    #         geography_level=spec.geography_level,
    #         state_fips=state_fips,
    #     )

    #     if not rows:
    #         self.logger.warning(
    #             "No data returned for %s vintage=%d state=%s",
    #             spec.name, vintage, state_fips,
    #         )
    #         return 0, 0

    #     # Add vintage column to each row
    #     for row in rows:
    #         row["vintage"] = vintage

    #     # Ingest via StagedIngest
    #     geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
    #     conflict_columns = geo_columns + ["vintage"]

    #     dataset_id = f"{spec.name}/{vintage}/{state_fips}"

    #     if self.tracker:
    #         with self.tracker.track(
    #             source="census_api",
    #             dataset_id=dataset_id,
    #             target_table=f"{spec.target_schema}.{spec.target_table}",
    #             metadata={
    #                 "vintage": vintage,
    #                 "state_fips": state_fips,
    #                 "geography_level": spec.geography_level,
    #                 "dataset": spec.dataset,
    #                 "num_variables": len(variables),
    #             },
    #         ) as run:
    #             staged, merged = self._ingest_rows(
    #                 spec, rows, conflict_columns,
    #             )
    #             run.rows_staged = staged
    #             run.rows_merged = merged
    #     else:
    #         staged, merged = self._ingest_rows(spec, rows, conflict_columns)

    #     self.logger.info(
    #         "Ingested %s vintage=%d state=%s: staged=%d merged=%d",
    #         spec.name, vintage, state_fips, staged, merged,
    #     )
    #     return staged, merged

    def _collect_state_vintage(
        self,
        spec: CensusDatasetSpec,
        vintage: int,
        state_fips: str,
        variables: list[str],
    ) -> tuple[int, int]:
        """
        Fetch and ingest data for one state + vintage combination.

        Returns (rows_staged, rows_merged).
        """
        self.logger.info(
            "Collecting %s vintage=%d state=%s",
            spec.name,
            vintage,
            state_fips,
        )

        # Fetch from Census API
        rows = self.client.fetch_variables(
            dataset=spec.dataset,
            vintage=vintage,
            variables=variables,
            geography_level=spec.geography_level,
            state_fips=state_fips,
        )

        if not rows:
            self.logger.warning(
                "No data returned for %s vintage=%d state=%s",
                spec.name,
                vintage,
                state_fips,
            )
            return 0, 0

        # Add vintage column to each row
        for row in rows:
            row["vintage"] = vintage

        # Ingest via StagedIngest (SCD2 mode)
        geo_columns = [
            self.client._sanitize_column_name(c)
            for c in GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
        ]
        entity_key = geo_columns + ["vintage"]

        dataset_id = f"{spec.name}/{vintage}/{state_fips}"

        if self.tracker:
            with self.tracker.track(
                source="census_api",
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table}",
                metadata={
                    "vintage": vintage,
                    "state_fips": state_fips,
                    "geography_level": spec.geography_level,
                    "dataset": spec.dataset,
                    "num_variables": len(variables),
                },
            ) as run:
                staged, merged = self._ingest_rows(
                    spec,
                    rows,
                    entity_key,
                )
                run.rows_staged = staged
                run.rows_merged = merged
        else:
            staged, merged = self._ingest_rows(spec, rows, entity_key)

        self.logger.info(
            "Ingested %s vintage=%d state=%s: staged=%d merged=%d",
            spec.name,
            vintage,
            state_fips,
            staged,
            merged,
        )
        return staged, merged

    # def _ingest_rows(
    #     self,
    #     spec: CensusDatasetSpec,
    #     rows: list[dict],
    #     conflict_columns: list[str],
    # ) -> tuple[int, int]:
    #     """Write rows via StagedIngest. Returns (rows_staged, rows_merged)."""
    #     with self.engine.staged_ingest(
    #         target_table=spec.target_table,
    #         target_schema=spec.target_schema,
    #         conflict_column=conflict_columns,
    #         conflict_action="UPDATE",
    #     ) as stager:
    #         stager.write_batch(rows)

    #     return stager.rows_staged, stager.rows_merged

    def _ingest_rows(
        self,
        spec: CensusDatasetSpec,
        rows: list[dict],
        entity_key: list[str],
    ) -> tuple[int, int]:
        """Write rows via StagedIngest in SCD2 mode. Returns (rows_staged, rows_merged)."""
        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            entity_key=entity_key,
            metadata_columns=METADATA_COLUMNS,
        ) as stager:
            stager.write_batch(rows)

        return stager.rows_staged, stager.rows_merged

    def _already_ingested(
        self,
        spec: CensusDatasetSpec,
        vintage: int,
        state_fips: str,
    ) -> bool:
        """Check if current (non-superseded) data exists for this spec+vintage+state."""
        fqn = f"{spec.target_schema}.{spec.target_table}"
        try:
            df = self.engine.query(
                f"""
                select 1 from {fqn}
                where vintage = %(vintage)s
                  and state = %(state)s
                  and "valid_to" is null
                limit 1
                """,
                {"vintage": vintage, "state": state_fips},
            )
            return not df.empty
        except Exception:
            # Table might not exist yet on first run
            return False

    def generate_ddl(self, spec: CensusDatasetSpec) -> str:
        return generate_ddl(spec=spec, client=self.client)


# def generate_ddl(
#     spec: CensusDatasetSpec,
#     client: CensusClient,
# ) -> str:
#     """
#     Generate a CREATE TABLE statement for a CensusDatasetSpec.

#     Resolves all groups and individual variables across every vintage in
#     the spec, takes the union, filters to estimate (E) and margin-of-error
#     (M) columns, and produces typed DDL. This ensures columns exist for
#     variables that may have been added or removed across vintages.

#     Parameters
#     ----------
#     spec : CensusDatasetSpec
#     client : CensusClient
#         Used to resolve group variables via the Census API.

#     Returns
#     -------
#     str
#         A CREATE TABLE SQL statement.
#     """
#     all_variables: set[str] = set()
#     for vintage in spec.vintages:
#         vintage_vars = client.resolve_all_variables(spec, vintage)
#         all_variables.update(vintage_vars)

#     # Filter to E and M suffixes only
#     variables = sorted(v for v in all_variables if _is_estimate_or_moe(v))

#     geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
#     fqn = f"{spec.target_schema}.{spec.target_table}"

#     lines = [f'create table {fqn} (']

#     # Geography ID columns
#     for col in geo_columns:
#         col_name = _sanitize_column_name(col)
#         lines.append(f'    "{col_name}" text not null,')

#     # Vintage
#     lines.append(f'    "vintage" integer not null,')

#     # NAME (returned by the API for every call)
#     lines.append(f'    "NAME" text,')

#     # Data columns
#     for var in variables:
#         col_type = _column_type(var)
#         lines.append(f'    "{var}" {col_type},')

#     # Ingested-at timestamp
#     lines.append(f'    "ingested_at" timestamptz not null default (now() at time zone \'UTC\')')

#     lines.append(');')

#     return "\n".join(lines)


# def generate_ddl(
#     spec: CensusDatasetSpec,
#     client: CensusClient,
# ) -> str:
#     """
#     Generate a CREATE TABLE statement for a CensusDatasetSpec.

#     Resolves all groups and individual variables across every vintage in
#     the spec, takes the union, filters to estimate (E) and margin-of-error
#     (M) columns, and produces typed DDL. This ensures columns exist for
#     variables that may have been added or removed across vintages.

#     Parameters
#     ----------
#     spec : CensusDatasetSpec
#     client : CensusClient
#         Used to resolve group variables via the Census API.

#     Returns
#     -------
#     str
#         A CREATE TABLE SQL statement.
#     """
#     all_variables: set[str] = set()
#     for vintage in spec.vintages:
#         vintage_vars = client.resolve_all_variables(spec, vintage)
#         all_variables.update(vintage_vars)

#     # Filter to E and M suffixes only, excluding NAME (which ends in E
#     # but is a geo label, not a data variable — it's added separately)
#     variables = sorted(
#         v for v in all_variables
#         if client._is_estimate_or_moe(v) and v != "NAME"
#     )

#     geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
#     fqn = f"{spec.target_schema}.{spec.target_table}"

#     lines = [f'create table {fqn} (']

#     # Geography ID columns
#     for col in geo_columns:
#         col_name = client._sanitize_column_name(col)
#         lines.append(f'    "{col_name}" text not null,')

#     # Vintage
#     lines.append(f'    "vintage" integer not null,')

#     # NAME (returned by the API for every call)
#     lines.append(f'    "NAME" text,')

#     # Data columns
#     for var in variables:
#         col_type = self.client._column_type(var)
#         lines.append(f'    "{var}" {col_type},')

#     # Ingested-at timestamp
#     lines.append(f'    "ingested_at" timestamptz not null default (now() at time zone \'UTC\')')

#     lines.append(');')

#     return "\n".join(lines)


def generate_ddl(
    spec: CensusDatasetSpec,
    client: CensusClient,
) -> str:
    """
    Generate a CREATE TABLE statement for a CensusDatasetSpec.

    Resolves all groups and individual variables across every vintage in
    the spec, takes the union, filters to estimate (E) and margin-of-error
    (M) columns, and produces typed DDL. This ensures columns exist for
    variables that may have been added or removed across vintages.

    Parameters
    ----------
    spec : CensusDatasetSpec
    client : CensusClient
        Used to resolve group variables via the Census API.

    Returns
    -------
    str
        A CREATE TABLE SQL statement.
    """
    all_variables: set[str] = set()
    for vintage in spec.vintages:
        vintage_vars = client.resolve_all_variables(spec, vintage)
        all_variables.update(vintage_vars)

    # Filter to E and M suffixes only, excluding NAME (which ends in E
    # but is a geo label, not a data variable — it's added separately)
    variables = sorted(v for v in all_variables if client._is_estimate_or_moe(v) and v != "NAME")

    geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
    entity_key = [client._sanitize_column_name(c) for c in geo_columns] + ["vintage"]
    fqn = f"{spec.target_schema}.{spec.target_table}"

    lines = [f"create table {fqn} ("]

    # Geography ID columns
    for col in geo_columns:
        col_name = client._sanitize_column_name(col)
        lines.append(f'    "{col_name}" text not null,')

    # Vintage
    lines.append('    "vintage" integer not null,')

    # NAME (returned by the API for every call)
    lines.append('    "NAME" text,')

    # Data columns
    for var in variables:
        col_type = client._column_type(var)
        lines.append(f'    "{var}" {col_type},')

    # Metadata / SCD2 columns
    lines.append("    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC'),")
    lines.append('    "record_hash" text not null,')
    lines.append("    \"valid_from\" timestamptz not null default (now() at time zone 'UTC'),")
    lines.append('    "valid_to" timestamptz')

    lines.append(");")

    # Unique constraint on entity key + record_hash (prevents duplicate versions)
    ek_cols = ", ".join(f'"{c}"' for c in entity_key)
    constraint_name = f"uq_{spec.target_table}_entity_hash"
    lines.append("")
    lines.append(f"alter table {fqn}")
    lines.append(f"    add constraint {constraint_name}")
    lines.append(f'    unique ({ek_cols}, "record_hash");')

    # Partial index for current versions
    index_name = f"ix_{spec.target_table}_current"
    lines.append("")
    lines.append(f"create index {index_name}")
    lines.append(f"    on {fqn} ({ek_cols})")
    lines.append('    where "valid_to" is null;')

    return "\n".join(lines)
