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

import geopandas as gpd
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

    # tile_id is written as data but excluded from the SCD2 hash so that
    # re-tiling a region doesn't create spurious new versions.
    HASH_EXCLUDE_COLUMNS = METADATA_COLUMNS | {"tile_id"}

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
    ) -> dict:
        """Download bike network tiles and ingest nodes and edges into PostGIS.

        Each tile is checked individually for freshness and skipped if it
        was ingested recently. This makes collection idempotent — a run
        that was interrupted can be resumed without re-ingesting tiles
        that already completed.

        Parameters
        ----------
        spec : OsmnxDatasetSpec
        force : bool
            If True, skip freshness checks and re-ingest all tiles.

        Returns
        -------
        dict with keys: spec_name, tiles (list of per-tile summaries).
        """
        tile_summaries = []

        for tile_id, nodes_gdf, edges_gdf in self.client.iter_bike_network_tiles(
            bbox=spec.bbox,
            max_tile_degrees=spec.max_tile_degrees,
            network_type=spec.network_type,
        ):
            if not force and self._already_ingested(spec, tile_id):
                self.logger.info("Skipping tile %s (already ingested recently)", tile_id)
                tile_summaries.append(
                    {
                        "tile_id": tile_id,
                        "skipped": True,
                        "nodes": None,
                        "edges": None,
                    }
                )
                continue

            self.logger.info("Ingesting tile %s", tile_id)
            nodes_summary = self._ingest_nodes(nodes_gdf, spec, tile_id)
            edges_summary = self._ingest_edges(edges_gdf, spec, tile_id)
            tile_summaries.append(
                {
                    "tile_id": tile_id,
                    "skipped": False,
                    "nodes": nodes_summary,
                    "edges": edges_summary,
                }
            )

        return {
            "spec_name": spec.name,
            "tiles": tile_summaries,
        }

    def _already_ingested(
        self,
        spec: OsmnxDatasetSpec,
        tile_id: str,
        max_months_stale: int = 3,
    ) -> bool:
        """Check if current, recently-ingested data exists for this tile."""
        try:
            node_fqn = f"{spec.target_schema}.{spec.target_table_nodes}"
            edge_fqn = f"{spec.target_schema}.{spec.target_table_edges}"
            node_df = self.engine.query(
                f"""
                select 1 from {node_fqn}
                where valid_to is null
                    and tile_id = %(tile_id)s
                    and ingested_at >= now() - interval '%(mms)s months'
                limit 1
                """,
                {"tile_id": tile_id, "mms": max_months_stale},
            )
            edge_df = self.engine.query(
                f"""
                select 1 from {edge_fqn}
                where valid_to is null
                    and tile_id = %(tile_id)s
                    and ingested_at >= now() - interval '%(mms)s months'
                limit 1
                """,
                {"tile_id": tile_id, "mms": max_months_stale},
            )
            return (not node_df.empty) and (not edge_df.empty)
        except Exception as e:
            self.logger.info(
                "Encountered error %s while checking past ingestions for tile %s",
                e,
                tile_id,
            )
            return False

    # ------------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------------

    def _ingest_nodes(
        self,
        nodes_gdf: gpd.GeoDataFrame,
        spec: OsmnxDatasetSpec,
        tile_id: str,
    ) -> dict:
        """Flatten and ingest a nodes GeoDataFrame for one tile."""
        self.logger.info("Ingesting %d nodes for tile %s", len(nodes_gdf), tile_id)

        dataset_id = f"{spec.name}/nodes"

        def _run():
            return self._write_rows_from_gdf(
                gdf=nodes_gdf,
                flatten_fn=lambda batch: self._flatten_nodes(batch, tile_id),
                target_table=spec.target_table_nodes,
                target_schema=spec.target_schema,
                entity_key=spec.entity_key_nodes,
                batch_size=spec.tile_batch_size,
            )

        if self.tracker:
            with self.tracker.track(
                source=self.SOURCE_NAME,
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table_nodes}",
                metadata={
                    "bbox": list(spec.bbox),
                    "tile_id": tile_id,
                    "network_type": spec.network_type,
                    "node_count": len(nodes_gdf),
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
        self.logger.info("Nodes ingest complete for tile %s: %s", tile_id, summary)
        return summary

    def _flatten_nodes(self, batch: gpd.GeoDataFrame, tile_id: str) -> list[dict]:
        """Convert a GeoDataFrame batch of nodes to a list of row dicts."""
        rows = []
        for _, row in batch.iterrows():
            geom = row.get("geometry")
            rows.append(
                {
                    "osmid": int(row["osmid"]),
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
                    "tile_id": tile_id,
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Edges
    # ------------------------------------------------------------------

    def _ingest_edges(
        self,
        edges_gdf: gpd.GeoDataFrame,
        spec: OsmnxDatasetSpec,
        tile_id: str,
    ) -> dict:
        """Flatten and ingest an edges GeoDataFrame for one tile."""
        self.logger.info("Ingesting %d edges for tile %s", len(edges_gdf), tile_id)

        dataset_id = f"{spec.name}/edges"

        def _run():
            return self._write_rows_from_gdf(
                gdf=edges_gdf,
                flatten_fn=lambda batch: self._flatten_edges(batch, tile_id),
                target_table=spec.target_table_edges,
                target_schema=spec.target_schema,
                entity_key=spec.entity_key_edges,
                batch_size=spec.tile_batch_size,
            )

        if self.tracker:
            with self.tracker.track(
                source=self.SOURCE_NAME,
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table_edges}",
                metadata={
                    "bbox": list(spec.bbox),
                    "tile_id": tile_id,
                    "network_type": spec.network_type,
                    "edge_count": len(edges_gdf),
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
        self.logger.info("Edges ingest complete for tile %s: %s", tile_id, summary)
        return summary

    def _flatten_edges(self, batch: gpd.GeoDataFrame, tile_id: str) -> list[dict]:
        """Convert a GeoDataFrame batch of edges to a list of row dicts."""
        rows = []
        for _, row in batch.iterrows():
            geom = row.get("geometry")

            osmid_raw = row.get("osmid")
            if isinstance(osmid_raw, list):
                osmid = str(osmid_raw)
            elif osmid_raw is not None:
                osmid = str(int(osmid_raw))
            else:
                osmid = None

            rows.append(
                {
                    "u": int(row["u"]),
                    "v": int(row["v"]),
                    "key": int(row["key"]),
                    "osmid": osmid,
                    "name": self._to_str_or_none(row.get("name")),
                    "highway": self._to_str_or_none(row.get("highway")),
                    "oneway": bool(row.get("oneway", False)),
                    "reversed": self._to_str_or_none(row.get("reversed")),
                    "length_m": float(row["length"]) if "length" in row else None,
                    "maxspeed": self._to_str_or_none(row.get("maxspeed")),
                    "surface": self._to_str_or_none(row.get("surface")),
                    "cycleway": self._to_str_or_none(row.get("cycleway")),
                    "cycleway_right": self._to_str_or_none(row.get("cycleway:right")),
                    "cycleway_left": self._to_str_or_none(row.get("cycleway:left")),
                    "bicycle": self._to_str_or_none(row.get("bicycle")),
                    "lanes": self._to_str_or_none(row.get("lanes")),
                    "ref": self._to_str_or_none(row.get("ref")),
                    "service": self._to_str_or_none(row.get("service")),
                    "width": self._to_str_or_none(row.get("width")),
                    "lit": self._to_str_or_none(row.get("lit")),
                    "access": self._to_str_or_none(row.get("access")),
                    "bridge": self._to_str_or_none(row.get("bridge")),
                    "tunnel": self._to_str_or_none(row.get("tunnel")),
                    "geom": geom.wkt if geom is not None else None,
                    "tile_id": tile_id,
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _write_rows_from_gdf(
        self,
        gdf: gpd.GeoDataFrame,
        flatten_fn,
        target_table: str,
        target_schema: str,
        entity_key: list[str],
        batch_size: int,
    ) -> tuple[int, int]:
        """Slice a GeoDataFrame into batches, flatten, and write via StagedIngest.

        Returns (rows_staged, rows_merged).
        """
        with self.engine.staged_ingest(
            target_table=target_table,
            target_schema=target_schema,
            entity_key=entity_key,
            metadata_columns=self.METADATA_COLUMNS,
            hash_exclude_columns=self.HASH_EXCLUDE_COLUMNS,
        ) as stager:
            for start in range(0, len(gdf), batch_size):
                batch = gdf.iloc[start : start + batch_size]
                rows = flatten_fn(batch)
                self.logger.info(
                    "Writing batch %d-%d of %d to %s.%s",
                    start,
                    start + len(batch),
                    len(gdf),
                    target_schema,
                    target_table,
                )
                stager.write_batch(rows)

        return stager.rows_staged, stager.rows_merged

    @staticmethod
    def _to_str_or_none(val) -> str | None:
        """Convert a value to string, handling lists (common in OSMnx) and NaN."""
        if val is None:
            return None
        if isinstance(val, list):
            return str(val)
        if isinstance(val, float):
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
            '"tile_id" text',
            # SCD2 metadata
            "\"ingested_at\" timestamptz not null default (now() at time zone 'UTC')",
            '"record_hash" text not null',
            "\"valid_from\" timestamptz not null default (now() at time zone 'UTC')",
            '"valid_to" timestamptz',
        ]

        lines = [f"create table {fqn} ("]
        lines.append("    " + ",\n    ".join(columns))
        lines.append(");")

        constraint_name = f"uq_{spec.target_table_nodes}_entity_hash"
        lines.append("")
        lines.append(f"alter table {fqn}")
        lines.append(f"    add constraint {constraint_name}")
        lines.append(f'    unique ({ek_cols}, "record_hash");')

        index_name = f"ix_{spec.target_table_nodes}_current"
        lines.append("")
        lines.append(f"create index {index_name}")
        lines.append(f"    on {fqn} ({ek_cols})")
        lines.append('    where "valid_to" is null;')

        lines.append("")
        lines.append(f"create index ix_{spec.target_table_nodes}_geom")
        lines.append(f"    on {fqn} using gist (geom);")

        lines.append("")
        lines.append(f"create index ix_{spec.target_table_nodes}_tile_id")
        lines.append(f"    on {fqn} (tile_id)")
        lines.append('    where "valid_to" is null;')

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
            '"reversed" text',
            '"length_m" double precision',
            '"maxspeed" text',
            '"surface" text',
            '"cycleway" text',
            '"cycleway_right" text',
            '"cycleway_left" text',
            '"bicycle" text',
            '"lanes" text',
            '"ref" text',
            '"service" text',
            '"width" text',
            '"lit" text',
            '"access" text',
            '"bridge" text',
            '"tunnel" text',
            '"geom" geometry(LineString, 4326)',
            '"tile_id" text',
            # SCD2 metadata
            "\"ingested_at\" timestamptz not null default (now() at time zone 'UTC')",
            '"record_hash" text not null',
            "\"valid_from\" timestamptz not null default (now() at time zone 'UTC')",
            '"valid_to" timestamptz',
        ]

        lines = [f"create table {fqn} ("]
        lines.append("    " + ",\n    ".join(columns))
        lines.append(");")

        constraint_name = f"uq_{spec.target_table_edges}_entity_hash"
        lines.append("")
        lines.append(f"alter table {fqn}")
        lines.append(f"    add constraint {constraint_name}")
        lines.append(f'    unique ({ek_cols}, "record_hash");')

        index_name = f"ix_{spec.target_table_edges}_current"
        lines.append("")
        lines.append(f"create index {index_name}")
        lines.append(f"    on {fqn} ({ek_cols})")
        lines.append('    where "valid_to" is null;')

        lines.append("")
        lines.append(f"create index ix_{spec.target_table_edges}_geom")
        lines.append(f"    on {fqn} using gist (geom);")

        lines.append("")
        lines.append(f"create index ix_{spec.target_table_edges}_u")
        lines.append(f"    on {fqn} (u)")
        lines.append('    where "valid_to" is null;')

        lines.append("")
        lines.append(f"create index ix_{spec.target_table_edges}_v")
        lines.append(f"    on {fqn} (v)")
        lines.append('    where "valid_to" is null;')

        lines.append("")
        lines.append(f"create index ix_{spec.target_table_edges}_tile_id")
        lines.append(f"    on {fqn} (tile_id)")
        lines.append('    where "valid_to" is null;')

        return "\n".join(lines)
