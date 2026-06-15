# /loci_platform/platform/airflow/dags/loci/collectors/threedep/collector.py
"""
USGS 3DEP elevation collection orchestrator.

Wires ThreeDEPDatasetSpec + ThreeDEPClient + the raster ingestion tooling into
the standard collector surface: collect(spec, force) and
generate_ddl(spec), all ingestion via StagedIngest (SCD2, keyed on
tile_id).

Update scheme:
    The unit of work is one 1-degree source tile. The seamless 3DEP
    products are essentially static (a tile is re-staged only when USGS
    republishes it), so:

      collect(spec, force=False)  downloads only 1-degree tiles that
          have no rows yet in the target, then ingests them. This is the
          cheap path: it avoids re-downloading hundreds of MB per tile
          that we already hold.
      collect(spec, force=True)   re-downloads every tile in the bbox
          and re-ingests. SCD2 dedupes unchanged sub-tiles away and
          versions any that USGS actually changed, so a forced run is
          how you pick up re-staged tiles.

    Limitation (deliberate, until needed): presence is judged by whether
    *any* sub-tile of a 1-degree tile exists in the target. A run that
    crashed partway through a tile would look "present" and be skipped on
    an incremental run; re-run with force=True to heal it. We accept this
    because the source rarely changes and a forced run is cheap to
    schedule periodically.

Each 1-degree tile is downloaded to a temp file (rasterio reads windows
off disk, so peak memory is one sub-tile), tiled and clipped to the
spec's bbox, then dropped. tile_id is namespaced by the 1-degree tile
name so sub-tiles stay unique across tiles: "<name>/<row>_<col>".
"""

from __future__ import annotations

import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loci.collectors.threedep.client import ThreeDEPClient, tiles_for_bbox
from loci.collectors.threedep.spec import THREEDEP_SRID, ThreeDEPDatasetSpec
from loci.raster.ingest import ingest_raster_file, raster_table_ddl
from loci.tracking.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)

# Sub-tile edge for the big 1-degree source rasters. 512 keeps a 1-degree
# 1/3 arc-second tile (~10812 px) to ~480 rows rather than ~1800 at 256,
# while staying small enough for fast point sampling.
DEFAULT_TILE_SIZE = 512
DEFAULT_BATCH_SIZE = 16


class ThreeDEPCollector:
    """
    Orchestrate 3DEP elevation collection.

    Parameters
    ----------
    engine : PostgresEngine
    client : ThreeDEPClient, optional
    tracker : IngestionTracker, optional
    tile_size : int
        Sub-tile edge in pixels passed to the raster ingester.
    batch_size : int
        Sub-tiles per COPY batch. Low by default — each is a large field.
    """

    SOURCE_NAME = "3dep"

    def __init__(
        self,
        engine,
        client: ThreeDEPClient | None = None,
        tracker: IngestionTracker | None = None,
        tile_size: int = DEFAULT_TILE_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.engine = engine
        self.client = client or ThreeDEPClient()
        self.tracker = tracker or IngestionTracker(engine=self.engine)
        self.tile_size = tile_size
        self.batch_size = batch_size
        self.logger = logging.getLogger("threedep_collector")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, spec: ThreeDEPDatasetSpec, force: bool = False) -> dict[str, Any]:
        """
        Collect 3DEP tiles covering spec.bbox into the target table.

        Raises if the target table doesn't exist (run print_ddl first).
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        # A missing target table is reported per tile (like the CMS/DKAN
        # collectors) rather than crashing the run; checked once here.
        table_ok = self._table_exists(spec)

        ingested_at = datetime.now(UTC)
        names = tiles_for_bbox(spec.bbox)
        mode = "full" if force else "incremental"

        self.logger.info(
            "Collecting %s: mode=%s, product=%s, %d tile(s) over bbox",
            spec.name,
            mode,
            spec.product,
            len(names),
        )

        summary: dict[str, Any] = {
            "spec_name": spec.name,
            "mode": mode,
            "product": spec.product,
            "tiles_total": len(names),
            "tiles_collected": 0,
            "tiles_skipped_present": 0,
            "tiles_missing_at_source": 0,
            "sub_tiles_read": 0,
            "rows_merged": 0,
            "errors": [],
        }

        run_metadata = {"mode": mode, "product": spec.product, "tiles_total": len(names)}
        with self.tracker.track(
            source=self.SOURCE_NAME,
            dataset_id=spec.name,
            target_table=fqn,
            metadata=run_metadata,
        ) as run:
            for name in names:
                try:
                    if not table_ok:
                        raise RuntimeError(
                            f"Target table {fqn} does not exist. "
                            f"Run print_ddl(spec) and create it first."
                        )
                    if not force and self._tile_present(spec, name):
                        self.logger.info("Skipping %s (already present)", name)
                        summary["tiles_skipped_present"] += 1
                        continue
                    if not self.client.tile_exists(name, spec.product):
                        self.logger.warning("Tile %s not available at source; skipping", name)
                        summary["tiles_missing_at_source"] += 1
                        continue

                    result = self._collect_tile(spec, name, ingested_at)
                    summary["tiles_collected"] += 1
                    summary["sub_tiles_read"] += result["tiles_read"]
                    summary["rows_merged"] += result["rows_merged"]
                except Exception as e:  # noqa: BLE001 - record and continue per tile
                    self.logger.error("Failed tile %s: %s", name, e)
                    summary["errors"].append({"tile": name, "error": str(e)})

            run.rows_merged = summary["rows_merged"]
            run.metadata.update(
                {
                    "tiles_collected": summary["tiles_collected"],
                    "tiles_skipped_present": summary["tiles_skipped_present"],
                    "tiles_missing_at_source": summary["tiles_missing_at_source"],
                    "sub_tiles_read": summary["sub_tiles_read"],
                }
            )

        self.logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    def generate_ddl(self, spec: ThreeDEPDatasetSpec) -> str:
        """CREATE TABLE for a 3DEP raster tile table (NAD83)."""
        return raster_table_ddl(spec.target_schema, spec.target_table, THREEDEP_SRID)

    def print_ddl(self, spec: ThreeDEPDatasetSpec) -> None:
        """Generate and print DDL for copy-paste into a migration."""
        print(self.generate_ddl(spec))

    # ------------------------------------------------------------------
    # Per-tile collection
    # ------------------------------------------------------------------

    def _collect_tile(
        self, spec: ThreeDEPDatasetSpec, name: str, ingested_at: datetime
    ) -> dict[str, int]:
        """Download one 1-degree tile to a temp file and ingest it, clipped to the bbox."""
        tmp = tempfile.NamedTemporaryFile(suffix=".tif", prefix=f"3dep_{name}_", delete=False)
        tmp.close()
        path = Path(tmp.name)
        try:
            self.client.download_tile(name, spec.product, path)
            return ingest_raster_file(
                self.engine,
                str(path),
                source_id=name,
                target_schema=spec.target_schema,
                target_table=spec.target_table,
                ingested_at=ingested_at,
                tile_size=self.tile_size,
                batch_size=self.batch_size,
                bounds=_bbox_bounds(spec),
            )
        finally:
            path.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Target-table probes
    # ------------------------------------------------------------------

    def _table_exists(self, spec: ThreeDEPDatasetSpec) -> bool:
        df = self.engine.query(
            """
            select 1 from information_schema.tables
            where table_schema = %(schema)s and table_name = %(table)s
            limit 1
            """,
            {"schema": spec.target_schema, "table": spec.target_table},
        )
        return not df.empty

    def _tile_present(self, spec: ThreeDEPDatasetSpec, name: str) -> bool:
        """True if any current sub-tile of this 1-degree tile exists in the target."""
        fqn = f"{spec.target_schema}.{spec.target_table}"
        df = self.engine.query(
            f"""
            select 1 from {fqn}
            where "valid_to" is null and "tile_id" like %(prefix)s
            limit 1
            """,
            {"prefix": f"{name}/%"},
        )
        return not df.empty


def _bbox_bounds(spec: ThreeDEPDatasetSpec) -> tuple[float, float, float, float]:
    """(min_x, min_y, max_x, max_y) clip extent in the tiles' CRS (NAD83)."""
    b = spec.bbox
    return (b.west, b.south, b.east, b.north)
