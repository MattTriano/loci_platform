"""
OsmnxCollector — collects bike network graph data from OSMnx and
ingests nodes and edges into PostGIS via StagedIngest (SCD2).

Usage:
    from loci.collectors.osmnx.client import OsmnxClient
    from loci.collectors.osmnx.spec import OsmnxDatasetSpec
    from loci.collectors.osmnx.collector import OsmnxCollector

    spec = OsmnxDatasetSpec(
        name="chicagoland_bike_network",
        target_table_nodes="osmnx_bike_network_nodes",
        target_table_edges="osmnx_bike_network_edges",
        bbox=(-87.97, 41.62, -87.5, 42.05),
    )

    client = OsmnxClient(cache_dir="/tmp/osmnx_cache")
    collector = OsmnxCollector(client=client, engine=engine)
    summary = collector.collect(spec)
"""

import logging

import osmnx as ox
from loci.collectors.osmnx.client import OsmnxClient
from loci.collectors.osmnx.spec import OsmnxDatasetSpec
from loci.tracking.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)


class OsmnxCollector:
    """Collects OSMnx bike network data and ingests into PostGIS.

    Parameters
    ----------
    engine : PostgresEngine
    client : OsmnxClient, optional
    tracker : IngestionTracker, optional
    """

    SOURCE_NAME = "osmnx"

    METADATA_COLUMNS = {
        "ingested_at",
        "record_hash",
        "valid_from",
        "valid_to",
    }

    def __init__(
        self,
        engine,
        client: OsmnxClient | None = None,
        tracker: IngestionTracker | None = None,
        logger: logging.Logger | None = None,
    ):
        self.engine = engine
        self.client = client or OsmnxClient()
        self.tracker = tracker or IngestionTracker(engine=self.engine)
        self.logger = logger or logging.getLogger("osmnx_collector")

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def collect(
        self,
        spec: OsmnxDatasetSpec,
        force: bool = False,
        batch_size: int = 5000,
    ) -> dict:
        """Download bike network and ingest nodes and edges into PostGIS.

        Parameters
        ----------
        spec : OsmnxDatasetSpec
        batch_size : int
            Rows per write_batch call.

        Returns
        -------
        dict with keys: spec_name, nodes, edges.
        """
        if not force and self._already_ingested(spec):
            self.logger.info("Skipping ingestion, (already ingested recently)")
            nodes_summary = {
                "table": f"{spec.target_schema}.{spec.target_table_nodes}",
                "rows_staged": 0,
                "rows_merged": 0,
            }
            edges_summary = {
                "table": f"{spec.target_schema}.{spec.target_table_edges}",
                "rows_staged": 0,
                "rows_merged": 0,
            }
            return {
                "spec_name": spec.name,
                "nodes": nodes_summary,
                "edges": edges_summary,
            }
        G = self.client.get_bike_network(
            bbox=spec.bbox,
            network_type=spec.network_type,
        )

        nodes_summary = self._ingest_nodes(G, spec, batch_size)
        edges_summary = self._ingest_edges(G, spec, batch_size)

        return {
            "spec_name": spec.name,
            "nodes": nodes_summary,
            "edges": edges_summary,
        }

    def _already_ingested(self, spec: OsmnxDatasetSpec, max_months_stale: int = 3) -> bool:
        """
        Check if current (non-superseded) data exists for this region.
        """
        try:
            node_fqn = f"{spec.target_schema}.{spec.target_table_nodes}"
            edge_fqn = f"{spec.target_schema}.{spec.target_table_edges}"
            node_df = self.engine.query(
                f"""
                select 1 from {node_fqn}
                where "valid_to" is null
                    and ingested_at >= now() - interval '%(mms)s months'
                limit 1
                """,
                {"mms": (max_months_stale,)},
            )
            edge_df = self.engine.query(
                f"""
                select 1 from {edge_fqn}
                where "valid_to" is null
                    and ingested_at >= now() - interval '%(mms)s months'
                limit 1
                """,
                {"mms": (max_months_stale,)},
            )
            return (not node_df.empty) and (not edge_df.empty)
        except Exception as e:
            self.logger.info("Encountered error %s while checking past ingestions", e)
            return False

    # ------------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------------

    def _ingest_nodes(
        self,
        G,
        spec: OsmnxDatasetSpec,
        batch_size: int,
    ) -> dict:
        """Convert graph nodes to rows and ingest via StagedIngest."""
        logger.info("Extracting nodes from graph (%d nodes)", G.number_of_nodes())

        gdf_nodes = ox.graph_to_gdfs(G, nodes=True, edges=False)
        rows = self._flatten_nodes(gdf_nodes)
        logger.info("Flattened %d node rows", len(rows))

        dataset_id = f"{spec.name}/nodes"

        def _run():
            return self._write_rows(
                rows=rows,
                target_table=spec.target_table_nodes,
                target_schema=spec.target_schema,
                entity_key=spec.entity_key_nodes,
                batch_size=batch_size,
            )

        if self.tracker:
            with self.tracker.track(
                source=self.SOURCE_NAME,
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table_nodes}",
                metadata={
                    "bbox": list(spec.bbox),
                    "network_type": spec.network_type,
                    "node_count": len(rows),
                },
            ) as run:
                staged, merged = _run()
                run.rows_staged = staged
                run.rows_merged = merged
        else:
            staged, merged = _run()

        summary = {
            "table": f"{spec.target_schema}.{spec.target_table_nodes}",
            "rows_staged": staged,
            "rows_merged": merged,
        }
        logger.info("Nodes ingest complete: %s", summary)
        return summary

    def _flatten_nodes(self, gdf_nodes) -> list[dict]:
        """Convert a GeoDataFrame of nodes to a list of row dicts."""
        rows = []
        for osmid, row in gdf_nodes.iterrows():
            geom = row.get("geometry")
            rows.append(
                {
                    "osmid": int(osmid),
                    "latitude": row.get("y"),
                    "longitude": row.get("x"),
                    "street_count": (
                        int(row["street_count"])
                        if "street_count" in row and row["street_count"] is not None
                        else None
                    ),
                    "highway": self._to_str_or_none(row.get("highway")),
                    "ref": self._to_str_or_none(row.get("ref")),
                    "geom": geom.wkt if geom is not None else None,
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Edges
    # ------------------------------------------------------------------

    def _ingest_edges(
        self,
        G,
        spec: OsmnxDatasetSpec,
        batch_size: int,
    ) -> dict:
        """Convert graph edges to rows and ingest via StagedIngest."""
        logger.info("Extracting edges from graph (%d edges)", G.number_of_edges())

        gdf_edges = ox.graph_to_gdfs(G, nodes=False, edges=True)
        rows = self._flatten_edges(gdf_edges)
        logger.info("Flattened %d edge rows", len(rows))

        dataset_id = f"{spec.name}/edges"

        def _run():
            return self._write_rows(
                rows=rows,
                target_table=spec.target_table_edges,
                target_schema=spec.target_schema,
                entity_key=spec.entity_key_edges,
                batch_size=batch_size,
            )

        if self.tracker:
            with self.tracker.track(
                source=self.SOURCE_NAME,
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table_edges}",
                metadata={
                    "bbox": list(spec.bbox),
                    "network_type": spec.network_type,
                    "edge_count": len(rows),
                },
            ) as run:
                staged, merged = _run()
                run.rows_staged = staged
                run.rows_merged = merged
        else:
            staged, merged = _run()

        summary = {
            "table": f"{spec.target_schema}.{spec.target_table_edges}",
            "rows_staged": staged,
            "rows_merged": merged,
        }
        logger.info("Edges ingest complete: %s", summary)
        return summary

    def _flatten_edges(self, gdf_edges) -> list[dict]:
        """Convert a GeoDataFrame of edges to a list of row dicts."""
        rows = []
        for (u, v, key), row in gdf_edges.iterrows():
            geom = row.get("geometry")

            # osmid can be a single int or a list for merged ways
            osmid_raw = row.get("osmid")
            if isinstance(osmid_raw, list):
                osmid = str(osmid_raw)
            elif osmid_raw is not None:
                osmid = str(int(osmid_raw))
            else:
                osmid = None

            rows.append(
                {
                    "u": int(u),
                    "v": int(v),
                    "key": int(key),
                    "osmid": osmid,
                    "name": self._to_str_or_none(row.get("name")),
                    "highway": self._to_str_or_none(row.get("highway")),
                    "oneway": bool(row.get("oneway", False)),
                    "length_m": float(row["length"]) if "length" in row else None,
                    "maxspeed": self._to_str_or_none(row.get("maxspeed")),
                    "surface": self._to_str_or_none(row.get("surface")),
                    "cycleway": self._to_str_or_none(row.get("cycleway")),
                    "cycleway_right": self._to_str_or_none(row.get("cycleway:right")),
                    "cycleway_left": self._to_str_or_none(row.get("cycleway:left")),
                    "bicycle": self._to_str_or_none(row.get("bicycle")),
                    "lanes": self._to_str_or_none(row.get("lanes")),
                    "width": self._to_str_or_none(row.get("width")),
                    "lit": self._to_str_or_none(row.get("lit")),
                    "access": self._to_str_or_none(row.get("access")),
                    "bridge": self._to_str_or_none(row.get("bridge")),
                    "tunnel": self._to_str_or_none(row.get("tunnel")),
                    "geom": geom.wkt if geom is not None else None,
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _write_rows(
        self,
        rows: list[dict],
        target_table: str,
        target_schema: str,
        entity_key: list[str],
        batch_size: int,
    ) -> tuple[int, int]:
        """Write rows to a target table via StagedIngest (SCD2).

        Returns (rows_staged, rows_merged).
        """
        with self.engine.staged_ingest(
            target_table=target_table,
            target_schema=target_schema,
            entity_key=entity_key,
            metadata_columns=self.METADATA_COLUMNS,
        ) as stager:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                logger.info(
                    "Writing batch %d-%d of %d to %s.%s",
                    i,
                    i + len(batch),
                    len(rows),
                    target_schema,
                    target_table,
                )
                stager.write_batch(batch)

        return stager.rows_staged, stager.rows_merged

    @staticmethod
    def _to_str_or_none(val) -> str | None:
        """Convert a value to string, handling lists (common in OSMnx)."""
        if val is None:
            return None
        if isinstance(val, list):
            return str(val)
        if isinstance(val, float):
            # NaN check (pandas NaN)
            import math

            if math.isnan(val):
                return None
        return str(val)

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def generate_ddl(self, spec: OsmnxDatasetSpec) -> str:
        """Generate CREATE TABLE statements for both nodes and edges tables."""
        nodes_ddl = self._generate_nodes_ddl(spec)
        edges_ddl = self._generate_edges_ddl(spec)
        return f"{nodes_ddl}\n\n\n{edges_ddl}"

    def _generate_nodes_ddl(self, spec: OsmnxDatasetSpec) -> str:
        fqn = f"{spec.target_schema}.{spec.target_table_nodes}"
        ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key_nodes)

        columns = [
            '"osmid" bigint not null',
            '"latitude" double precision',
            '"longitude" double precision',
            '"street_count" integer',
            '"highway" text',
            '"ref" text',
            '"geom" geometry(Point, 4326)',
            # SCD2 metadata
            "\"ingested_at\" timestamptz not null default (now() at time zone 'UTC')",
            '"record_hash" text not null',
            "\"valid_from\" timestamptz not null default (now() at time zone 'UTC')",
            '"valid_to" timestamptz',
        ]

        lines = [f"create table {fqn} ("]
        lines.append("    " + ",\n    ".join(columns))
        lines.append(");")

        # Unique constraint for SCD2
        constraint_name = f"uq_{spec.target_table_nodes}_entity_hash"
        lines.append("")
        lines.append(f"alter table {fqn}")
        lines.append(f"    add constraint {constraint_name}")
        lines.append(f'    unique ({ek_cols}, "record_hash");')

        # Partial index for current versions
        index_name = f"ix_{spec.target_table_nodes}_current"
        lines.append("")
        lines.append(f"create index {index_name}")
        lines.append(f"    on {fqn} ({ek_cols})")
        lines.append('    where "valid_to" is null;')

        # Spatial index
        lines.append("")
        lines.append(f"create index ix_{spec.target_table_nodes}_geom")
        lines.append(f"    on {fqn} using gist (geom);")

        return "\n".join(lines)

    def _generate_edges_ddl(self, spec: OsmnxDatasetSpec) -> str:
        fqn = f"{spec.target_schema}.{spec.target_table_edges}"
        ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key_edges)

        columns = [
            '"u" bigint not null',
            '"v" bigint not null',
            '"key" integer not null',
            '"osmid" text',
            '"name" text',
            '"highway" text',
            '"oneway" boolean',
            '"length_m" double precision',
            '"maxspeed" text',
            '"surface" text',
            '"cycleway" text',
            '"cycleway_right" text',
            '"cycleway_left" text',
            '"bicycle" text',
            '"lanes" text',
            '"width" text',
            '"lit" text',
            '"access" text',
            '"bridge" text',
            '"tunnel" text',
            '"geom" geometry(LineString, 4326)',
            # SCD2 metadata
            "\"ingested_at\" timestamptz not null default (now() at time zone 'UTC')",
            '"record_hash" text not null',
            "\"valid_from\" timestamptz not null default (now() at time zone 'UTC')",
            '"valid_to" timestamptz',
        ]

        lines = [f"create table {fqn} ("]
        lines.append("    " + ",\n    ".join(columns))
        lines.append(");")

        # Unique constraint for SCD2
        constraint_name = f"uq_{spec.target_table_edges}_entity_hash"
        lines.append("")
        lines.append(f"alter table {fqn}")
        lines.append(f"    add constraint {constraint_name}")
        lines.append(f'    unique ({ek_cols}, "record_hash");')

        # Partial index for current versions
        index_name = f"ix_{spec.target_table_edges}_current"
        lines.append("")
        lines.append(f"create index {index_name}")
        lines.append(f"    on {fqn} ({ek_cols})")
        lines.append('    where "valid_to" is null;')

        # Spatial index
        lines.append("")
        lines.append(f"create index ix_{spec.target_table_edges}_geom")
        lines.append(f"    on {fqn} using gist (geom);")

        # Index for graph reconstruction (lookup edges by node)
        lines.append("")
        lines.append(f"create index ix_{spec.target_table_edges}_u")
        lines.append(f"    on {fqn} (u)")
        lines.append('    where "valid_to" is null;')

        lines.append("")
        lines.append(f"create index ix_{spec.target_table_edges}_v")
        lines.append(f"    on {fqn} (v)")
        lines.append('    where "valid_to" is null;')

        return "\n".join(lines)
