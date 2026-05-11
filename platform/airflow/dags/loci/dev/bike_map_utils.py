# loci_platform/platform/airflow/dags/loci/exports/bike_map_layers.py
"""
Per-city bike-map layer display config + a helper for drafting it.

A GeoJSONExportConfig says *what* gets exported (table, geometry,
properties). A LayerDisplayConfig says *how* the bike-map frontend
should display it: the layer label, color, popup field order, named
formatters, and which field (if any) is the date used for filtering.

The two are paired by name: LayerDisplayConfig.export_name must match
some GeoJSONExportConfig.name in the same CityBuildSpec.

For drafting a LayerDisplayConfig from a real mart table and rendering
it for paste into either Python or the app's static config.json, see
LayerConfigHelper at the bottom of this file.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from loci.exports.bike_map_layers import LayerDisplayConfig, PopupField
from loci.exports.geojson_export import GeoJSONExportConfig

if TYPE_CHECKING:
    from loci.db.core import PostgresEngine


# ───────────────────────────────────────────────────────────────────
# LayerConfigHelper: drafting + rendering helpers
# ───────────────────────────────────────────────────────────────────


# A small, ordered palette used as a deterministic fallback for draft
# layer colors. The first N drafts in a session get the first N colors.
_DRAFT_COLOR_PALETTE = [
    "#ef4444",  # red
    "#f59e0b",  # amber
    "#22c55e",  # green
    "#3b82f6",  # blue
    "#8b5cf6",  # violet
    "#ec4899",  # pink
]

# Columns we usually don't want to surface in popups even when they're
# in the export properties. Drafts skip these; you can still add them
# back by hand if needed.
_DEFAULT_POPUP_EXCLUDES = frozenset(
    {
        "id",
        "source_id",
        "crash_record_id",
        "geom",
        "geometry",
        "the_geom",
        "latitude",
        "longitude",
    }
)


@dataclass
class LayerConfigHelper:
    """Helper for drafting and rendering bike-map layer configs.

    Inspects a real mart table (via Postgres) to draft a starting
    LayerDisplayConfig from a GeoJSONExportConfig. The draft is meant
    as a copy-pasteable starting point — labels, color, popup_title,
    and field ordering generally need human refinement.

    Three methods, each doing one thing:
      - draft_layer_display: inspect a table and return a draft
        LayerDisplayConfig.
      - format_as_python_literal: render a draft (or final)
        LayerDisplayConfig as a Python literal to paste into a per-city
        Python module.
      - format_for_app_config: render a final list of LayerDisplayConfigs
        as a JSON block to paste into apps/bike-map/config/{env}/{city}.json.
    """

    engine: PostgresEngine
    schema: str

    def draft_layer_display(
        self,
        export: GeoJSONExportConfig,
        *,
        color: str | None = None,
        default_visible: bool = True,
    ) -> LayerDisplayConfig:
        """Draft a LayerDisplayConfig for the given export by inspecting
        the mart table.

        Most of the value is in the popup_fields list and the
        date_field guess. label, popup_title, and color are placeholders
        you'll typically edit by hand.

        Args:
            export: The GeoJSONExportConfig whose table to inspect.
            color: Override the auto-picked palette color.
            default_visible: Passed through to LayerDisplayConfig.

        Raises:
            ValueError if the table can't be inspected.
        """
        col_types = self._fetch_column_types(export.table)

        # Restrict to the columns that will actually be exported.
        # If properties is None, the exporter sends every non-geometry
        # column, so we use everything we found.
        if export.properties:
            export_cols = [c for c in export.properties if c in col_types]
        else:
            export_cols = list(col_types.keys())

        date_field = self._guess_date_field(export_cols, col_types)
        popup_fields = self._draft_popup_fields(export.table, export_cols, col_types)

        # popup_title is a placeholder you'll typically edit (e.g. you
        # want 'Bike Crash' rather than 'Bike Crashes' as a heading).
        # We default it to the label to avoid a brittle singularizer.
        label = _humanize(export.name)
        return LayerDisplayConfig(
            export_name=export.name,
            label=label,
            color=color or self._pick_color(),
            popup_title=label,
            popup_fields=popup_fields,
            date_field=date_field,
            default_visible=default_visible,
        )

    def format_as_python_literal(self, ld: LayerDisplayConfig) -> str:
        """Render a LayerDisplayConfig as a Python literal.

        The output is meant to paste into a per-city Python module like
        dag_files/refresh_bike_map_chicago_layers.py. Imports of
        LayerDisplayConfig and PopupField are assumed; this only
        produces the LayerDisplayConfig(...) call itself.
        """
        lines = ["LayerDisplayConfig("]
        lines.append(f"    export_name={ld.export_name!r},")
        lines.append(f"    label={ld.label!r},")
        lines.append(f"    color={ld.color!r},")
        lines.append(f"    popup_title={ld.popup_title!r},")
        if ld.date_field is not None:
            lines.append(f"    date_field={ld.date_field!r},")
        if not ld.default_visible:
            lines.append("    default_visible=False,")
        if ld.cluster_max_zoom != 13:
            lines.append(f"    cluster_max_zoom={ld.cluster_max_zoom},")
        if ld.cluster_radius != 45:
            lines.append(f"    cluster_radius={ld.cluster_radius},")
        if ld.point_radius != 5:
            lines.append(f"    point_radius={ld.point_radius},")

        if ld.popup_fields:
            lines.append("    popup_fields=[")
            for pf in ld.popup_fields:
                fmt_part = f", fmt={pf.fmt!r}" if pf.fmt else ""
                lines.append(f"        PopupField(key={pf.key!r}, label={pf.label!r}{fmt_part}),")
            lines.append("    ],")
        else:
            lines.append("    popup_fields=[],")

        lines.append(")")
        return "\n".join(lines)

    def format_for_app_config(
        self,
        layer_displays: list[LayerDisplayConfig],
        *,
        indent: int = 2,
    ) -> str:
        """Render the layers list as a JSON string ready for the app config.

        print(layer_helper.format_for_app_config(CHICAGO_LAYER_DISPLAYS))

        Paste the output into the "layers" key of
        apps/bike-map/config/{env}/{city}.json.
        """
        payload = [_layer_display_to_dict(ld) for ld in layer_displays]
        return json.dumps(payload, indent=indent)

    # ── inspection helpers ─────────────────────────────────────────

    def _fetch_column_types(self, table: str) -> dict[str, str]:
        """Return {column_name: data_type} from information_schema."""
        sql = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %(schema)s AND table_name = %(table)s
            ORDER BY ordinal_position
        """
        df = self.engine.query(sql, {"schema": self.schema, "table": table})
        if df.empty:
            raise ValueError(f"Table {self.schema}.{table} has no columns (or doesn't exist)")
        return dict(zip(df["column_name"], df["data_type"], strict=True))

    def _guess_date_field(self, cols: list[str], col_types: dict[str, str]) -> str | None:
        """Pick the most likely date column, or None.

        Heuristic: prefer 'date'-typed columns over 'timestamp'-typed.
        Within each, prefer columns whose name contains 'date'.
        """
        date_cols = [c for c in cols if col_types.get(c) == "date"]
        if date_cols:
            named = [c for c in date_cols if "date" in c.lower()]
            return named[0] if named else date_cols[0]

        ts_cols = [c for c in cols if col_types.get(c, "").startswith("timestamp")]
        if ts_cols:
            named = [c for c in ts_cols if "date" in c.lower() or "time" in c.lower()]
            return named[0] if named else ts_cols[0]

        return None

    def _draft_popup_fields(
        self,
        table: str,
        cols: list[str],
        col_types: dict[str, str],
    ) -> list[PopupField]:
        """Build a draft list of popup fields, skipping ID-ish columns
        and inferring formatters from column names + types."""
        fields = []
        for col in cols:
            if col in _DEFAULT_POPUP_EXCLUDES:
                continue
            fmt = self._infer_formatter(table, col, col_types.get(col, ""))
            fields.append(PopupField(key=col, label=_humanize(col), fmt=fmt))
        return fields

    def _infer_formatter(self, table: str, col: str, col_type: str) -> str | None:
        """Return a formatter name for a column, or None."""
        if col_type == "boolean":
            return "yes_no_bool"

        # Y/N columns in this codebase are conventionally suffixed _i.
        if col.endswith("_i"):
            sample = self._sample_distinct(table, col, limit=5)
            if sample is not None and sample.issubset({"Y", "N", None}):
                return "yes_no_yn"

        if col.endswith("_hour") and col_type in {"integer", "smallint", "bigint"}:
            return "hour"

        return None

    def _sample_distinct(self, table: str, col: str, limit: int) -> set | None:
        """Sample distinct values from a column for type heuristics.

        Returns None on any error (caller treats absence as 'don't infer').
        """
        try:
            sql = f'SELECT DISTINCT "{col}" AS v FROM {self.schema}.{table} LIMIT {limit}'
            df = self.engine.query(sql)
            return set(df["v"].tolist())
        except Exception:
            return None

    def _pick_color(self) -> str:
        """Pick the next color from the palette.

        Uses a per-instance counter so successive draft calls cycle
        through the palette. Wraps at the palette length.
        """
        idx = getattr(self, "_color_idx", 0)
        color = _DRAFT_COLOR_PALETTE[idx % len(_DRAFT_COLOR_PALETTE)]
        self._color_idx = idx + 1
        return color


# ── module-level helpers used by both draft and format paths ──────────


def _layer_display_to_dict(ld: LayerDisplayConfig) -> dict:
    """Serialize one LayerDisplayConfig as the JSON shape the frontend reads."""
    out = {
        "id": ld.export_name,
        "label": ld.label,
        "color": ld.color,
        "file": f"data/{ld.export_name}.geojson",
        "popup_title": ld.popup_title,
        "popup_fields": [
            {"key": pf.key, "label": pf.label, **({"fmt": pf.fmt} if pf.fmt else {})}
            for pf in ld.popup_fields
        ],
        "default_visible": ld.default_visible,
        "cluster_max_zoom": ld.cluster_max_zoom,
        "cluster_radius": ld.cluster_radius,
        "point_radius": ld.point_radius,
    }
    if ld.date_field is not None:
        out["date_field"] = ld.date_field
    return out


_HUMANIZE_RE = re.compile(r"[_\s]+")


def _humanize(name: str) -> str:
    """snake_case -> Title Case, with a couple of tweaks for our column
    naming conventions:

      - Trailing '_i' (indicator suffix) is dropped: 'hit_and_run_i' ->
        'Hit And Run'.
      - Trailing '_id' is left alone since it's still meaningful.
    """
    name = name.removesuffix("_i") if name.endswith("_i") and not name.endswith("_id") else name
    return _HUMANIZE_RE.sub(" ", name).strip().title()
